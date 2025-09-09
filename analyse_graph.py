from py2neo import Graph

class GraphAnalyser:
    def __init__(self, uri="http://localhost:7474", user="neo4j", password="Neo4jade"):
        self.graph = Graph(uri, auth=(user, password))

    @staticmethod
    # 解决百分比除以零问题
    def _pct(x):
        x = x or 0.0
        try:
            return f"{float(x):.2%}"
        except Exception:
            return "0.00%"

    # Top-3 热门商品session级转化率
    def top_products_by_views(self, topn=3):
        q = """
        MATCH (e:Event)-[:ABOUT]->(p:Product) // 匹配所有关于商品的事件
        RETURN p.product_id AS pid, count(*) AS views // 按商品统计浏览量
        ORDER BY views DESC 
        LIMIT $n
        """
        return self.graph.run(q, n=int(topn)).data()

    def product_conversion_session(self, pid):
        q = """
        MATCH (p:Product {product_id:$pid})
        // 看过该产品的session
        MATCH (s:Session)-[:CONTAINS]->(:Event)-[:ABOUT]->(p)
        WITH p, collect(DISTINCT s) AS S_view // 收集所有看过该商品的独立会话

        // 这些会话中，是否有“关于该产品”的购买
        UNWIND S_view AS s 
        OPTIONAL MATCH (s)-[:CONTAINS]->(e:Event)-[:ABOUT]->(p)
        OPTIONAL MATCH (e)-[:RESULTED_IN]->(:Outcome) // 关联购买结果
        WITH S_view, s, count(CASE WHEN exists(e.event_id) THEN 1 END) AS bought_cnt
        WITH S_view, s, CASE WHEN bought_cnt>0 THEN 1 ELSE 0 END AS bought_flag
        RETURN
          size(S_view) AS sessions_seen,
          sum(bought_flag) AS sessions_bought,
          CASE WHEN size(S_view)=0 THEN 0.0
               ELSE 1.0*sum(bought_flag)/size(S_view) END AS conversion_rate
        """
        row = self.graph.run(q, pid=str(pid)).data()
        return row[0] if row else {"sessions_seen": 0, "sessions_bought": 0, "conversion_rate": 0.0}

    def report_top3_with_conversion(self):
        rows = self.top_products_by_views(3)
        print(f"\nTop 3 Products (by views) & Conversion (session-level)")
        print("─" * 72)
        if not rows:
            print("No products found.")
            print("─" * 72)
            return
        print(f"{'Rank':<6} {'ProductID':<20} {'Views':>10} {'Conversion':>14}")
        print("─" * 72)
        for i, r in enumerate(rows, start=1):
            pid, views = r["pid"], r["views"]
            conv = self.product_conversion_session(pid)
            conv_rate = self._pct(conv.get("conversion_rate"))
            print(f"{i:<6} {pid:<20} {views:>10} {conv_rate:>14}")
        print("─" * 72)


    # Top 5 顾客
    def top_customers_by_purchases(self, topn=5):
        q = """
        MATCH (u:User)-[:STARTED]->(:Session)-[:CONTAINS]->(e:Event)-[:RESULTED_IN]->(:Outcome)
        OPTIONAL MATCH (e)-[:ABOUT]->(p:Product)
        WITH u, count(e) AS purchases, collect(DISTINCT p.product_id) AS prod_set
        RETURN u.user_id AS uid, purchases, prod_set
        ORDER BY purchases DESC
        LIMIT $n
        """
        return self.graph.run(q, n=int(topn)).data()

    def report_top5_customers(self):
        rows = self.top_customers_by_purchases(5)
        print(f"\nTop 5 Customers by Purchases")
        print("─" * 48)
        if not rows:
            print("No customers found.")
            print("─" * 48)
            return
        print(f"{'Rank':<6} {'UserID':<16} {'Purchases':>10} ")
        print("─" * 48)
        for i, r in enumerate(rows, start=1):
            uid = r["uid"]
            buys = r["purchases"] or 0

            print(f"{i:<6} {uid:<16} {buys:>10} ")
        print("─" * 48)


    # “通向购买”的高概率路径（再往前两步 → 共 4 步在购买之前）
    def two_steps_far_before_purchase(self, topk=10, product_id=None):
        """
        统计 s-4 → s-3 → s-2 → s-1 → purchase 的路径，输出 2,3（最后一步是purchase）
        可选按 product_id 过滤
        """
        q = """
        MATCH (e0:Event)-[:RESULTED_IN]->(:Outcome)
        OPTIONAL MATCH (e0)-[:ABOUT]->(pp:Product)
        WITH e0, pp
        WHERE ($pid IS NULL OR pp.product_id = $pid)

        MATCH (e4:Event)-[:NEXT]->(e3:Event)-[:NEXT]->(e2:Event)-[:NEXT]->(e1:Event)-[:NEXT]->(e0)
        RETURN
          e3.type_raw AS s3,
          e2.type_raw AS s2,
          count(*)    AS occurrences
        ORDER BY occurrences DESC
        LIMIT $k
        """
        return self.graph.run(q, k=int(topk), pid=product_id).data()

    def report_two_steps_far_before_purchase(self, topk=10, product_id=None):
        title = f"Two-Step Patterns  Top {topk}"
        if product_id:
            title += f"  [Product={product_id}]"
        print("\n" + title)
        print("─" * 84)
        rows = self.two_steps_far_before_purchase(topk=topk, product_id=product_id)
        if not rows:
            print("No paths found.")
            print("─" * 84)
            return

        # 计算占比（分母=本次结果 occurrences 总和）
        total = sum((r["occurrences"] or 0) for r in rows) or 1
        print(f"{'Before 2':<24} {'Before 1':<24} {'Count':>10} {'Share':>10}")
        print("-" * 84)
        for r in rows:
            s3 = r["s3"] or "N/A"
            s2 = r["s2"] or "N/A"
            cnt = r["occurrences"] or 0
            pct = f"{cnt / total:.2%}"
            print(f"{s3:<24} {s2:<24} {cnt:>10} {pct:>10}")
        print("─" * 84)

    # 漏斗分析：S_purchase ⊆ S_cart ⊆ S_click ⊆ S_view
    def funnel(self, product_id=None):
        q = """
        // View
        OPTIONAL MATCH (pp:Product {product_id:$pid})
        MATCH (s:Session)-[:CONTAINS]->(v:Event)
        OPTIONAL MATCH (v)-[:ABOUT]->(pv:Product)
        WITH collect(DISTINCT CASE WHEN v.type_raw IN ['product_view','view','page_view']
                                   AND ($pid IS NULL OR pv = pp) THEN s END) AS S_view, pp

        // Click
        MATCH (s1:Session)-[:CONTAINS]->(c:Event)
        OPTIONAL MATCH (c)-[:ABOUT]->(pc:Product)
        WITH S_view, pp,
             collect(DISTINCT CASE WHEN s1 IN S_view AND c.type_raw='click'
                                   AND ($pid IS NULL OR pc = pp) THEN s1 END) AS S_click

        // AddToCart
        MATCH (s2:Session)-[:CONTAINS]->(a:Event)
        OPTIONAL MATCH (a)-[:ABOUT]->(pa:Product)
        WITH S_view, S_click, pp,
             collect(DISTINCT CASE WHEN s2 IN S_click AND a.type_raw='add_to_cart'
                                   AND ($pid IS NULL OR pa = pp) THEN s2 END) AS S_cart

        // Purchase
        MATCH (s3:Session)-[:CONTAINS]->(e3:Event)-[:RESULTED_IN]->(:Outcome)
        OPTIONAL MATCH (e3)-[:ABOUT]->(ppp:Product)
        WITH S_view, S_click, S_cart, pp,
             collect(DISTINCT CASE WHEN s3 IN S_cart AND ($pid IS NULL OR ppp = pp) THEN s3 END) AS S_purchase

        RETURN
          size([x IN S_view     WHERE x IS NOT NULL]) AS view_sessions,
          size([x IN S_click    WHERE x IS NOT NULL]) AS click_sessions,
          size([x IN S_cart     WHERE x IS NOT NULL]) AS cart_sessions,
          size([x IN S_purchase WHERE x IS NOT NULL]) AS purchase_sessions
        """
        row = self.graph.run(q, pid=product_id).data()
        return row[0] if row else {"view_sessions": 0, "click_sessions": 0, "cart_sessions": 0, "purchase_sessions": 0}

    def report_funnel(self, product_id=None):
        row = self.funnel(product_id)
        v, c, a, p = row["view_sessions"], row["click_sessions"], row["cart_sessions"], row["purchase_sessions"]

        def r(num, den): return (num / den) if den else 0.0

        title = "Funnel (cohort): View → Click → AddToCart → Purchase"
        if product_id: title += f" [Product={product_id}]"
        print(f"\n{title}\n{'─' * 80}")
        print(f"{'Stage':<20} {'Sessions':>12} {'Step Conv.':>14} {'Cumulative':>14}")
        print("-" * 80)
        print(f"{'View':<20} {v:>12} {'-':>14} {'-':>14}")
        print(f"{'Click':<20} {c:>12} {self._pct(r(c, v)):>14} {self._pct(r(c, v)):>14}")
        print(f"{'AddToCart':<20} {a:>12} {self._pct(r(a, c)):>14} {self._pct(r(a, v)):>14}")
        print(f"{'Purchase':<20} {p:>12} {self._pct(r(p, a)):>14} {self._pct(r(p, v)):>14}")
        print("─" * 80)


if __name__ == "__main__":
    z = GraphAnalyser()

    # Top-3 热门商品 + 会话级转化率
    z.report_top3_with_conversion()

    # Top-5 顾客
    z.report_top5_customers()

    # 购买前路径
    z.report_two_steps_far_before_purchase(topk=10, product_id=None)

    # 漏斗（全局/单品）
    z.report_funnel()


