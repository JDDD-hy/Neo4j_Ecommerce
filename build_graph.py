import json
from datetime import datetime, timezone
from collections import defaultdict
from py2neo import Graph, Node, Relationship
from tqdm import tqdm


class ECommerceGraph:
    def __init__(self, uri, user, password, data_path="data/ecommerce_all.json"):
        self.data_path = data_path
        self.graph = Graph(uri, auth=(user, password))

    def clear_graph(self):
        self.graph.delete_all()

    @staticmethod
    def to_iso(ts: str):
        if not ts:
            return None
        try:
            dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
            return dt.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        except Exception:
            return ts

    @staticmethod
    def sec_delta(t1: str, t2: str) -> int:
        dt1 = datetime.fromisoformat(t1.replace("Z", "+00:00"))
        dt2 = datetime.fromisoformat(t2.replace("Z", "+00:00"))
        return max(int((dt2 - dt1).total_seconds()), 0)

    def read_nodes(self):
        """
        读取 JSON 文件，提取节点和关系
        每条记录包含：UserID, SessionID, Timestamp, EventType, ProductID, Amount, Outcome
        """

        # 节点容器
        users = set()       # (:User {user_id})
        sessions = set()    # (:Session {session_id})
        events = []         # (:Event {event_id, event_type, timestamp})
        products = set()    # (:Product {product_id})
        outcomes = {"purchase"}  # 只有 purchase 这一类

        # 关系容器
        rels_user_session = set()   # (User)-[STARTED]->(Session)
        rels_session_event = []     # (Session)-[CONTAINS]->(Event)
        rels_next = []              # (Event)-[NEXT {delta_s}]->(Event)
        rels_about = []             # (Event)-[ABOUT]->(Product)
        rels_event_outcome = []     # (Event)-[RESULTED_IN {amount}]->(Outcome)

        # 读 JSON 数据
        with open(self.data_path, "r", encoding="utf-8") as f:
            rows = json.load(f)

        # 按 user/session 分桶
        buckets = defaultdict(list)  # (user_id, session_id) -> [records]
        for r in rows:
            user_id = str(r["UserID"])
            sess_local = str(r["SessionID"])
            session_id = f"{user_id}_{sess_local}"
            buckets[(user_id, session_id)].append(r)

        # 遍历每个用户的每个 session
        for (user_id, session_id), items in buckets.items():
            users.add(user_id)
            sessions.add(session_id)
            rels_user_session.add((user_id, session_id))

            # 按时间升序
            items.sort(key=lambda x: x["Timestamp"])
            last_eid, last_ts = None, None

            for i, r in enumerate(items, start=1):
                ts = self.to_iso(r["Timestamp"])
                etype = r["EventType"]
                pid = r.get("ProductID")
                pid = None if pid in (None, "", "null") else str(pid)

                eid = f"{session_id}#{i:04d}"

                # Event 节点信息
                events.append({
                    "event_id": eid,
                    "session_id": session_id,
                    "ts": ts,
                    "type_raw": etype
                })
                rels_session_event.append((session_id, eid))

                # NEXT
                if last_eid and last_ts and ts:
                    rels_next.append((last_eid, eid, self.sec_delta(last_ts, ts)))
                last_eid, last_ts = eid, ts

                # ABOUT
                if pid:
                    products.add(pid)
                    rels_about.append((eid, pid))

                # RESULTED_IN (purchase)
                if etype == "purchase":
                    amt = r.get("Amount")
                    try:
                        amt = float(amt) if amt is not None else None
                    except Exception:
                        amt = None
                    rels_event_outcome.append((eid, "purchase", amt))

        rels_user_session = [list(t) for t in rels_user_session]

        return {
            "users": users,
            "sessions": sessions,
            "events": events,
            "products": products,
            "outcomes": list(outcomes),
            "rels_user_session": rels_user_session,
            "rels_session_event": rels_session_event,
            "rels_next": rels_next,
            "rels_about": rels_about,
            "rels_event_outcome": rels_event_outcome
        }

    def create_node(self, label, nodes):
        """
        创建节点
        """
        if label == "Event":
            for ev in tqdm(nodes, desc="Creating Event nodes"):
                props = {
                    "event_id": ev.get("event_id"),
                    "session_id": ev.get("session_id"),
                    "type_raw": ev.get("type_raw"),
                }
                if ev.get("ts"):
                    props["ts"] = ev["ts"]
                self.graph.merge(Node("Event", **props), "Event", "event_id")

        elif label == "User":
            for uid in tqdm(nodes, desc="Creating User nodes"):
                self.graph.merge(Node("User", user_id=str(uid)), "User", "user_id")

        elif label == "Session":
            for sid in tqdm(nodes, desc="Creating Session nodes"):
                self.graph.merge(Node("Session", session_id=str(sid)), "Session", "session_id")

        elif label == "Product":
            for pid in tqdm(nodes, desc="Creating Product nodes"):
                props = {"product_id": str(pid), "name": str(pid)}
                self.graph.merge(Node("Product", **props), "Product", "product_id")

        elif label == "Outcome":
            for name in tqdm(nodes, desc="Creating Outcome nodes"):
                self.graph.merge(Node("Outcome", name=str(name)), "Outcome", "name")

        else:
            raise ValueError(f"Unknown label: {label}")

    # 创建知识图谱实体节点类型schema
    def create_graphnodes(self):
        data = self.read_nodes()
        self.create_node("User",data["users"])
        self.create_node("Session",data["sessions"])
        self.create_node("Product",data["products"])
        self.create_node("Outcome",data["outcomes"])
        self.create_node("Event",data["events"])
        return

    def create_relationship(self,start_node_label, end_node_label,edges, rel_type, rel_name):
        cnt = 0
        # 去重
        set_edges = []
        for edge in edges:
            set_edges.append('###'.join(edge))
        total = len(set(set_edges))
        for edge in set(set_edges):
            edge = edge.split('###')
            node1 = edge[0]
            node2 = edge[1]
            query = "match (p:%s),(q:%s) where p.name = '%s' and q.name = '%s' create (p)-[rel:%s{name:'%s'}]->(q)" % (
                start_node_label, end_node_label, node1, node2, rel_type,rel_name)
            try:
                self.graph.run(query)
                cnt += 1
                print(f"{cnt}/{total}. {rel_type} rel: {rel_name}")
            except Exception as e:
                print(e)

    def create_graphrels(self):
        """
        幂等创建 5 类关系：节点不存在时，先创建节点；关系已存在时，不重复创建
        STARTED / CONTAINS / NEXT / ABOUT / RESULTED_IN
        """
        data = self.read_nodes()

        # (User)-[:STARTED]->(Session)
        for uid, sid in tqdm(data["rels_user_session"], desc="STARTED rels"):
            u = Node("User", user_id=str(uid))
            s = Node("Session", session_id=str(sid))
            self.graph.merge(u, "User", "user_id")
            self.graph.merge(s, "Session", "session_id")
            self.graph.merge(Relationship(u, "STARTED", s))

        # (Session)-[:CONTAINS]->(Event)
        for sid, eid in tqdm(data["rels_session_event"], desc="CONTAINS rels"):
            s = Node("Session", session_id=str(sid))
            e = Node("Event", event_id=str(eid))
            self.graph.merge(s, "Session", "session_id")
            self.graph.merge(e, "Event", "event_id")
            self.graph.merge(Relationship(s, "CONTAINS", e))

        # (Event)-[:NEXT {delta_s}]->(Event)
        for prev_eid, next_eid, delta_s in tqdm(data["rels_next"], desc="NEXT rels"):
            e1 = Node("Event", event_id=str(prev_eid))
            e2 = Node("Event", event_id=str(next_eid))
            self.graph.merge(e1, "Event", "event_id")
            self.graph.merge(e2, "Event", "event_id")
            self.graph.merge(Relationship(e1, "NEXT", e2, delta_s=int(delta_s)))

        # (Event)-[:ABOUT]->(Product)
        for eid, pid in tqdm(data["rels_about"], desc="ABOUT rels"):
            e = Node("Event", event_id=str(eid))
            p = Node("Product", product_id=str(pid))
            self.graph.merge(e, "Event", "event_id")
            self.graph.merge(p, "Product", "product_id")
            self.graph.merge(Relationship(e, "ABOUT", p))

        # (Event)-[:RESULTED_IN {amount}]->(Outcome)
        for eid, _name, amt in tqdm(data["rels_event_outcome"], desc="RESULTED_IN rels"):
            e = Node("Event", event_id=str(eid))
            o = Node("Outcome", event_id=str(eid), name="purchase",
                     amount=0.0 if amt in (None, "") else float(amt))
            self.graph.merge(e, "Event", "event_id")
            self.graph.merge(o, "Outcome", "event_id")
            self.graph.merge(Relationship(e, "RESULTED_IN", o, amount=o["amount"]))

    def cleanup_global_outcomes(self):
        self.graph.run("""
        MATCH (o:Outcome)
        WHERE NOT exists(o.event_id)
        DETACH DELETE o
        """)

if __name__ == "__main__":
    graph_builder = ECommerceGraph(
        uri="http://localhost:7474/",
        user="neo4j",
        password="Neo4j",
        data_path="data/ecommerce_all.json"
    )
    graph_builder.clear_graph()
    graph_builder.create_graphnodes()
    graph_builder.create_graphrels()
    graph_builder.cleanup_global_outcomes()
