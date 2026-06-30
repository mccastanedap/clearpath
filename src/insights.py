import json
import anthropic
import src.config  # noqa: F401  -- ensures .env is loaded before anthropic reads ANTHROPIC_API_KEY


def generate_insights(top_products_df, daily_revenue_df, velocity_df, business_name, business_type):
    """
    Ask Claude for a short headline and 3 recommended next steps, returned as:
        {"headline": str, "steps": [{"title": str, "description": str}, ...]}
    The email's numbers are computed from the data in main.py; the LLM only writes
    the prose. Falls back to safe defaults if the model returns nothing parseable,
    so the email always has content.
    """
    client = anthropic.Anthropic()
    prompt = f"""
You are a data analyst helping a small business owner understand their sales.

Business: {business_name}
Type: {business_type}

TOP SELLING PRODUCTS:
{top_products_df.to_string()}

DAILY REVENUE (last 30 days):
{daily_revenue_df.to_string()}

PRODUCT VELOCITY (units sold per day - LOW means spoilage risk):
{velocity_df.to_string()}

Write:
1. "headline": one short, friendly clause (max ~10 words) about the standout product or trend this week. It will follow "Hi {business_name}, " so make it flow. No numbers or dollar signs.
2. "steps": exactly 3 actionable recommendations for this week. Each has a short "title" (max ~6 words) and a one-sentence "description" in plain language. Use the velocity data to flag dead stock if relevant.

Keep the language simple - this owner is not a data person.

Return ONLY valid JSON in exactly this shape, with no markdown, no code fences, no extra text:
{{"headline": "...", "steps": [{{"title": "...", "description": "..."}}, {{"title": "...", "description": "..."}}, {{"title": "...", "description": "..."}}]}}
"""
    fallback = {
        "headline": "here's your weekly snapshot.",
        "steps": [
            {"title": "Review your top sellers",
             "description": "Keep your best-selling products well stocked this week."},
            {"title": "Watch slow movers",
             "description": "Check low-velocity products to avoid spoilage and dead stock."},
            {"title": "Plan your week",
             "description": "Use this report to decide what to order and what to promote."},
        ],
    }

    try:
        message = client.messages.create(
            model="claude-sonnet-4-6",
            max_tokens=1024,
            messages=[{"role": "user", "content": prompt}],
        )
        raw = message.content[0].text.strip()
        if raw.startswith("```"):
            raw = raw.strip("`")
            raw = raw[raw.find("{"):raw.rfind("}") + 1]
        data = json.loads(raw)
        headline = str(data.get("headline") or fallback["headline"]).strip()
        steps = []
        for s in (data.get("steps") or [])[:3]:
            title = str(s.get("title", "")).strip()
            desc = str(s.get("description", "")).strip()
            if title and desc:
                steps.append({"title": title, "description": desc})
        if not steps:
            steps = fallback["steps"]
        return {"headline": headline, "steps": steps}
    except Exception as e:
        print(f"Insights generation fell back to defaults: {e}")
        return fallback
