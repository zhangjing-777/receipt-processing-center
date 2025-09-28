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

