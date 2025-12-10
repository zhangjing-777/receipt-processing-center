import json
from decimal import Decimal
from typing import Dict
from collections import defaultdict


def normalize_category(cat: str, seller: str = "") -> str:
    """将发票的 category/seller 映射到标准化的报销分类"""
    c = (cat or "").strip().lower()
    s = (seller or "").strip().lower()
    text = f"{c} {s}"

    # --- Transportation ---
    if any(k in text for k in [
        "taxi", "uber", "didi", "cabify", "bolt",
        "metro", "bus", "tram", "subway",
        "train", "rail", "high-speed", "hsr",
        "flight", "air", "plane", "air ticket", "airport",
        "toll", "fuel", "gas", "petrol", "car rental"
    ]):
        return "Transportation"

    # --- Accommodation ---
    if any(k in text for k in [
        "hotel", "lodging", "inn", "hostel", "airbnb", "motel"
    ]):
        return "Accommodation"

    # --- Meals & Entertainment ---
    if any(k in text for k in [
        "meal", "food", "restaurant", "canteen", "cafeteria", 
        "dining", "lunch", "dinner", "breakfast",
        "banquet", "entertainment", "client dinner"
    ]):
        return "Meals & Entertainment"

    # --- Conference & Training ---
    if any(k in text for k in [
        "conference", "seminar", "workshop", "training", "registration", "expo", "fair"
    ]):
        return "Conference & Training"

    # --- Office & Supplies ---
    if any(k in text for k in [
        "office", "supplies", "stationery", "pen", "paper", "notebook",
        "printing", "print", "photocopy", "scan",
        "courier", "express", "shipping", "postage", "delivery"
    ]):
        return "Office & Supplies"

    # --- Communication ---
    if any(k in text for k in [
        "phone", "mobile", "sim", "internet", "wifi", "data plan", "telecom"
    ]):
        return "Communication"

    # --- Fallback ---
    return "Others"


def aggregate_by_buyer(invoices: list[dict]) -> dict:
    buyers = {}
    for idx, inv in enumerate(invoices, start=1):
        buyer = inv["buyer"]
        cur = inv["currency"]
        cat = normalize_category(inv["category"], inv.get("seller", ""))
        amt = Decimal(str(inv["invoice_total"]))  # 用 Decimal 防止浮点误差

        if buyer not in buyers:
            buyers[buyer] = {
                "by_cat": defaultdict(lambda: defaultdict(Decimal)),  # {cat: {cur: sum}}
                "by_currency": defaultdict(Decimal),                  # {cur: sum}
                "rows": []                                            # 明细行（表格用）
            }

        buyers[buyer]["by_cat"][cat][cur] += amt
        buyers[buyer]["by_currency"][cur] += amt

        buyers[buyer]["rows"].append({
            "Invoice Date": inv["invoice_date"],
            "Category": cat,  # 用规范化后的分类
            "Seller": inv.get("seller", ""),
            "Buyer": buyer,
            "Invoice Total": str(amt),   # 输出时再加货币符号
            "Currency": cur,
            "File URL": inv["file_url"]
        })
    return buyers

def serialize_for_invoices(invoices: list[dict]) -> dict:
    """把 aggregate_by_buyer 的结果转成干净的 JSON 可供 LLM 使用"""
    agg_result = aggregate_by_buyer(invoices)
    buyers_out = {}
    for buyer, data in agg_result.items():
        buyers_out[buyer] = {
            "totals_by_category": {
                cat: {cur: str(val) for cur, val in cur_map.items()}
                for cat, cur_map in data["by_cat"].items()
            },
            "totals_by_currency": {cur: str(val) for cur, val in data["by_currency"].items()},
            "rows": data["rows"],  # 已经是普通 dict list
        }  
    return buyers_out



def format_currency(amount: str, currency: str) -> str:
    symbols = {"CNY": "¥", "USD": "$", "EUR": "€"}
    symbol = symbols.get(currency, currency + " ")
    return f"{symbol}{amount}"

def group_rows_by_category(rows):
    grouped = defaultdict(list)
    for r in rows:
        grouped[r["Category"]].append(r)
    return grouped

def group_by_date_seller(rows):
    grouped = defaultdict(lambda: {"count": 0, "total": Decimal("0.00")})
    for r in rows:
        key = (r["Invoice Date"], r["Seller"])
        grouped[key]["count"] += 1
        grouped[key]["total"] += Decimal(r["Invoice Total"])
    return grouped

def describe_category(cat, rows, totals_by_currency):
    descs = []
    for cur, amt in totals_by_currency.items():
        total_str = format_currency(amt, cur)

        if cat == "Transportation":
            # 按日期+卖家分组
            grouped = group_by_date_seller(rows)
            parts = []
            for (date, seller), info in sorted(grouped.items()):
                parts.append(
                    f"{info['count']} ride(s)/ticket(s) with {seller} on {date} totaling {format_currency(str(info['total']), cur)}"
                )
            detail = "; ".join(parts)
            descs.append(f"- Transportation expenses totaling {total_str}, including {detail}.")

        elif cat == "Accommodation":
            sellers = {r["Seller"] for r in rows}
            dates = sorted({r["Invoice Date"] for r in rows})
            nights = len(dates)
            detail = f"{nights} night(s) at {', '.join(sellers)} between {dates[0]} and {dates[-1]}"
            descs.append(f"- Accommodation expenses totaling {total_str}, covering {detail}.")

        elif cat == "Meals & Entertainment":
            grouped = group_by_date_seller(rows)
            parts = []
            for (date, seller), info in sorted(grouped.items()):
                parts.append(
                    f"{info['count']} meal(s) at {seller} on {date} totaling {format_currency(str(info['total']), cur)}"
                )
            detail = "; ".join(parts)
            descs.append(f"- Meals & Entertainment expenses totaling {total_str}, including {detail}.")

        else:
            sellers = {r["Seller"] for r in rows}
            dates = sorted({r["Invoice Date"] for r in rows})
            detail = f"from {', '.join(sellers)} on {', '.join(dates)}"
            descs.append(f"- {cat} expenses totaling {total_str}, {detail}.")

    return descs

def render_summary(buyers_json: Dict) -> str:
    output_parts = []

    for buyer, data in buyers_json.items():
        part = []
        part.append(f"✅ Your business travel reimbursement summary for {buyer} has been generated:")

        # Category totals
        for cat, cur_map in data["totals_by_category"].items():
            for cur, amt in cur_map.items():
                part.append(f"- {cat}: {format_currency(amt, cur)}")
        part.append("")

        # Totals by currency
        part.append("Totals by currency:")
        for cur, amt in data["totals_by_currency"].items():
            part.append(f"- {cur}: {format_currency(amt, cur)}")
        part.append("")

        # Remarks
        part.append("Please copy the following description into the reimbursement remarks section:")
        part.append("During this business trip, the following expenses were incurred:")

        rows_by_cat = group_rows_by_category(data["rows"])
        for cat, rows in rows_by_cat.items():
            descs = describe_category(cat, rows, data["totals_by_category"][cat])
            part.extend(descs)

        part.append("All receipts have been attached. Please proceed with the review.\n")

        # Table
        part.append(f"Please find the details for {buyer} below:")
        headers = ["ID", "Invoice Date", "Category", "Seller", "Buyer", "Invoice Total", "Currency", "File URL"]
        part.append("| " + " | ".join(headers) + " |")
        part.append("|" + "|".join(["----"] * len(headers)) + "|")

        rows = sorted(data["rows"], key=lambda r: r["Invoice Date"])
        for idx, r in enumerate(rows, start=1):
            part.append("| " + " | ".join([
                str(idx),
                r["Invoice Date"],
                r["Category"],
                r["Seller"],
                r["Buyer"],
                r["Invoice Total"],
                r["Currency"],
                r["File URL"]
            ]) + " |")

        output_parts.append("\n".join(part))
        output_parts.append("\n")

    return "\n".join(output_parts)


def render_summary_html(buyers_json: Dict) -> str:
    """生成 HTML 格式的汇总报告（带交互式表格）"""
    output_parts = []
    
    # 添加 CSS 样式
    html_header = """
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; }
        h2 { color: #2c3e50; }
        .summary-section { margin-bottom: 40px; }
        table { border-collapse: collapse; width: 100%; margin-top: 20px; }
        th { background-color: #3498db; color: white; padding: 12px; text-align: left; }
        td { border: 1px solid #ddd; padding: 10px; }
        tr:nth-child(even) { background-color: #f2f2f2; }
        tr:hover { background-color: #e8f4f8; }
        input[type="number"] { width: 80px; padding: 5px; border: 1px solid #ccc; border-radius: 4px; }
        .amount { font-weight: bold; color: #27ae60; }
        .remarks { background-color: #fff3cd; padding: 15px; border-left: 4px solid #ffc107; margin: 20px 0; }
        .category-list { margin: 10px 0; }
    </style>
    <script>
    function calculateAmount(rowId) {
        const row = document.getElementById('row-' + rowId);
        const total = parseFloat(row.querySelector('.total').textContent) || 0;
        const rate = parseFloat(row.querySelector('.rate').value) || 0;
        const amount = total * rate;
        row.querySelector('.amount').textContent = amount.toFixed(2);
    }
    </script>
    """
    
    output_parts.append(html_header)

    for buyer, data in buyers_json.items():
        part = []
        part.append(f'<div class="summary-section">')
        part.append(f'<h2>✅ Your business travel reimbursement summary for {buyer} has been generated:</h2>')

        # Category totals
        part.append('<div class="category-list">')
        for cat, cur_map in data["totals_by_category"].items():
            for cur, amt in cur_map.items():
                part.append(f"<div>- {cat}: {format_currency(amt, cur)}</div>")
        part.append('</div>')

        # Totals by currency
        part.append('<h3>Totals by currency:</h3>')
        part.append('<div class="category-list">')
        for cur, amt in data["totals_by_currency"].items():
            part.append(f"<div>- {cur}: {format_currency(amt, cur)}</div>")
        part.append('</div>')

        # Remarks
        part.append('<div class="remarks">')
        part.append('<h3>Please copy the following description into the reimbursement remarks section:</h3>')
        part.append('<p>During this business trip, the following expenses were incurred:</p>')

        rows_by_cat = group_rows_by_category(data["rows"])
        for cat, rows in rows_by_cat.items():
            descs = describe_category(cat, rows, data["totals_by_category"][cat])
            for desc in descs:
                part.append(f'<p>{desc}</p>')

        part.append('<p>All receipts have been attached. Please proceed with the review.</p>')
        part.append('</div>')

        # Table with interactive inputs
        part.append(f'<h3>Please find the details for {buyer} below:</h3>')
        part.append('<table>')
        part.append('<thead><tr>')
        headers = ["ID", "Invoice Date", "Category", "Seller", "Buyer", "Invoice Total", "Currency", "Target Rate", "Target Amount", "File URL"]
        for header in headers:
            part.append(f'<th>{header}</th>')
        part.append('</tr></thead>')
        part.append('<tbody>')

        rows = sorted(data["rows"], key=lambda r: r["Invoice Date"])
        for idx, r in enumerate(rows, start=1):
            part.append(f'<tr id="row-{idx}">')
            part.append(f'<td>{idx}</td>')
            part.append(f'<td>{r["Invoice Date"]}</td>')
            part.append(f'<td>{r["Category"]}</td>')
            part.append(f'<td>{r["Seller"]}</td>')
            part.append(f'<td>{r["Buyer"]}</td>')
            part.append(f'<td class="total">{r["Invoice Total"]}</td>')
            part.append(f'<td>{r["Currency"]}</td>')
            part.append(f'<td><input type="number" class="rate" step="0.0001" placeholder="0.00" oninput="calculateAmount({idx})"></td>')
            part.append(f'<td class="amount">0.00</td>')
            part.append(f'<td><a href="{r["File URL"]}" target="_blank">View</a></td>')
            part.append('</tr>')

        part.append('</tbody>')
        part.append('</table>')
        part.append('</div>')

        output_parts.append("\n".join(part))

    return "\n".join(output_parts)