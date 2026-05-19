"""
Portfolio Analysis using LangChain + Ollama (local LLM)
Provides financial analysis for a portfolio of stocks with sentiment, news, and recommendations.
"""

import json
import logging
from typing import Any

from langchain_core.prompts import PromptTemplate
from langchain_community.llms import Ollama
from pydantic import BaseModel

log = logging.getLogger("screener")

# Portfolio of 9 Indian blue-chip stocks to analyze
DEFAULT_PORTFOLIO = [
    "Reliance Industries Ltd.",
    "Tata Consultancy Services Ltd.",
    "Bharti Airtel Ltd.",
    "Axis Bank Ltd.",
    "Hindustan Unilever Ltd.",
    "ITC Ltd.",
    "Larsen & Toubro Ltd.",
    "Zomato Ltd.",
    "Adani Green Energy Ltd.",
]


class StockSentiment(BaseModel):
    """Individual stock sentiment analysis"""
    symbol: str
    sector: str
    short_name: str
    sentiment: str  # "Positive", "Neutral", "Negative"
    sentiment_score: float  # -1.0 to 1.0
    confidence: int  # 0-100
    analyst_expectations: str
    summary: str
    key_metrics: list[str]
    news_items: list[dict[str, str]]  # {title, source, time}


class PortfolioAnalysisResponse(BaseModel):
    """Response model for portfolio analysis"""
    overall_health: str
    stock_sentiments: list[StockSentiment]
    sentiment_table: list[dict[str, str]]
    stocks_requiring_review: dict[str, dict[str, Any]]
    important_news: list[dict[str, str]]


def _init_ollama_llm(model_name: str = "deepseek-r1:1.5b", temperature: float = 0.3) -> Ollama:
    """Initialize Ollama LLM instance.
    
    Requires Ollama to be running locally on http://localhost:11434
    """
    try:
        llm = Ollama(model=model_name, temperature=temperature, base_url="http://localhost:11434")
        # Test connection
        llm("ping")
        log.info(f"Connected to Ollama model: {model_name}")
        return llm
    except Exception as e:
        log.error(f"Failed to connect to Ollama: {e}. Ensure Ollama is running locally.")
        raise


def analyze_portfolio(portfolio: list[str] | None = None, stock_details: dict[str, dict] | None = None) -> PortfolioAnalysisResponse:
    """
    Analyze a portfolio of stocks using LangChain + Ollama.
    
    Args:
        portfolio: List of stock names to analyze. Uses DEFAULT_PORTFOLIO if None.
        stock_details: Optional dict with stock symbol as key and details (price, score, etc.) as value.
        
    Returns:
        PortfolioAnalysisResponse with structured analysis
    """
    if portfolio is None:
        portfolio = DEFAULT_PORTFOLIO
    
    try:
        llm = _init_ollama_llm()
    except Exception as e:
        log.error(f"Portfolio analysis failed: {e}")
        raise
    
    # Create analysis prompt
    portfolio_str = "\n".join(f"- {stock}" for stock in portfolio)
    
    # Include stock details if provided
    details_str = ""
    if stock_details:
        details_str = "\n\nStock Performance Details:\n"
        for symbol, details in stock_details.items():
            score = details.get("score", "N/A")
            signal = details.get("signal", "N/A")
            short_name = details.get("short_name", symbol)
            details_str += f"- {short_name} ({symbol}): Score={score}, Signal={signal}\n"
    
    analysis_prompt = PromptTemplate(
        input_variables=["portfolio", "details"],
        template="""You are a financial analyst specializing in Indian equity markets. Analyze the following portfolio of stocks:

{portfolio}
{details}

Provide detailed sentiment analysis in the following JSON format:

{{
    "overall_health": "A 1-2 sentence assessment (Healthy/Mixed/Challenging/Reasonably Healthy with Areas Requiring Attention)",
    "stock_sentiments": [
        {{
            "symbol": "Stock Symbol (NSE ticker)",
            "sector": "Sector Name",
            "short_name": "Stock Name",
            "sentiment": "Positive/Neutral/Negative",
            "sentiment_score": 0.5,
            "confidence": 75,
            "analyst_expectations": "Positive/Buy or Mixed/Hold or Negative/Sell",
            "summary": "2-3 line summary of recent news and outlook",
            "key_metrics": ["Metric 1", "Metric 2", "Metric 3"],
            "news_items": [
                {{"title": "News headline", "source": "News Source", "time": "Time ago"}}
            ]
        }}
    ],
    "sentiment_table": [
        {{"stock": "Stock Name", "sentiment": "Good/Neutral/Needs Review", "analyst_expectations": "Brief phrase"}},
        ...
    ],
    "stocks_requiring_review": {{
        "Stock Name": {{
            "focus_areas": [
                {{"area": "Focus Area Name", "why_review": "Why this needs review based on recent news and market conditions"}}
            ]
        }}
    }},
    "important_news": [
        {{"number": 1, "summary": "Relevant news for portfolio stocks", "implication": "Key implication for investment"}},
        ...
    ]
}}

For each stock:
- sentiment_score: -1.0 (very negative) to 1.0 (very positive)
- confidence: 0-100, based on news coverage and consensus
- Include 2-3 key metrics (Q4 PAT growth, NIM, P/E ratio, etc.)
- Include 2-3 recent news items from financial media

Focus on:
1. Recent sector trends (Banking, IT, Energy, etc.)
2. Regulatory or market developments
3. Earnings expectations and actual results
4. Valuation concerns vs growth potential
5. News sentiment from financial media

Return ONLY valid JSON, no additional text.""",
    )
    
    # Run analysis
    log.info(f"Analyzing portfolio: {', '.join(portfolio[:3])}...")
    
    chain = analysis_prompt | llm
    
    try:
        result = chain.invoke({"portfolio": portfolio_str, "details": details_str})
        
        # Parse JSON response
        analysis_data = json.loads(result)
        
        # Parse stock sentiments
        stock_sentiments_data = analysis_data.get("stock_sentiments", [])
        stock_sentiments = []
        for stock_data in stock_sentiments_data:
            try:
                stock_sentiments.append(StockSentiment(**stock_data))
            except Exception as e:
                log.warning(f"Failed to parse stock sentiment: {e}")
        
        response = PortfolioAnalysisResponse(
            overall_health=analysis_data.get("overall_health", "Unable to assess"),
            stock_sentiments=stock_sentiments,
            sentiment_table=analysis_data.get("sentiment_table", []),
            stocks_requiring_review=analysis_data.get("stocks_requiring_review", {}),
            important_news=analysis_data.get("important_news", []),
        )
        
        log.info("Portfolio analysis completed successfully")
        return response
        
    except json.JSONDecodeError as e:
        log.error(f"Failed to parse LLM response as JSON: {e}")
        raise ValueError("LLM response was not valid JSON")
    except Exception as e:
        log.error(f"Portfolio analysis error: {e}")
        raise


def format_analysis_markdown(analysis: PortfolioAnalysisResponse) -> str:
    """Format analysis response as markdown for display."""
    md = f"""# Portfolio Analysis

## Overall Portfolio Health
{analysis.overall_health}

## Stock Sentiment & Analyst Expectations

| Stock | Sentiment | Analyst Expectations |
|-------|-----------|----------------------|
"""
    for item in analysis.sentiment_table:
        md += f"| {item.get('stock', 'N/A')} | {item.get('sentiment', 'N/A')} | {item.get('analyst_expectations', 'N/A')} |\n"
    
    md += "\n### Stocks Requiring Further Review\n\n"
    for stock, details in analysis.stocks_requiring_review.items():
        md += f"**{stock}**\n"
        for area in details.get("focus_areas", []):
            md += f"- {area.get('area', 'N/A')}\n"
            md += f"  - Why review is needed: {area.get('why_review', 'N/A')}\n"
    
    md += "\n### 10 Important News Pieces from the Last Week\n\n"
    for news in analysis.important_news:
        md += f"{news.get('number', '?')}. {news.get('summary', 'N/A')}\n"
        md += f"   - **Implication:** {news.get('implication', 'N/A')}\n\n"
    
    return md
