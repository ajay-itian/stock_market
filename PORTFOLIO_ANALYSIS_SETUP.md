# Portfolio Analysis Feature - Setup Guide

## Overview
This feature uses **LangChain + Ollama** (local LLM) to analyze a portfolio of 9 Indian blue-chip stocks, providing sentiment analysis, analyst expectations, and important market news.

## Prerequisites

### 1. Install Ollama
Ollama runs a local LLM without requiring external API keys.

**Download & Install:**
- Visit: https://ollama.ai
- Download for Windows, Mac, or Linux
- Install and follow setup instructions

### 2. Pull the LLM Model
After Ollama is installed and running, pull the `deepseek-r1:1.5b` model:

```bash
ollama pull deepseek-r1:1.5b
```

This downloads (~1.5GB). You only need to do this once.

### 3. Verify Ollama is Running
```bash
# Should respond with "Ollama is running"
curl http://localhost:11434/api/status
```

## Installation

### 1. Update Backend Dependencies
```bash
cd backend
pip install -r requirements.txt
```

New packages added:
- `langchain>=0.1.0`
- `langchain-community>=0.1.0`
- `ollama>=0.1.0`

### 2. Restart Backend
```bash
# In backend terminal
python main.py
```

## Usage

### 1. Trigger Portfolio Analysis (Backend)
```bash
curl -X GET http://localhost:8000/api/v1/portfolio/analysis \
  -H "X-Admin-Api-Key: your_api_key_if_required"
```

### 2. Response Format
```json
{
  "status": "ok",
  "data": {
    "overall_health": "Reasonably Healthy with Areas Requiring Attention",
    "sentiment_table": [
      {
        "stock": "Reliance Industries Ltd.",
        "sentiment": "Good",
        "analyst_expectations": "Positive/Buy"
      },
      ...
    ],
    "stocks_requiring_review": {
      "Stock Name": {
        "focus_areas": [
          {
            "area": "Focus Area Name",
            "why_review": "Why this needs review..."
          }
        ]
      }
    },
    "important_news": [
      {
        "number": 1,
        "summary": "News summary",
        "implication": "Key implication for portfolio"
      },
      ...
    ]
  }
}
```

### 3. Frontend Integration (Optional)
Add to your React app to display portfolio analysis:

```typescript
// In screener-ui/src/api.ts
export async function fetchPortfolioAnalysis(): Promise<any> {
  return apiFetch('/v1/portfolio/analysis');
}
```

Then in a React component:
```tsx
const [analysis, setAnalysis] = useState(null);

useEffect(() => {
  fetchPortfolioAnalysis()
    .then(res => setAnalysis(res.data))
    .catch(e => console.error(e));
}, []);
```

## Troubleshooting

### "Connection refused" error
- **Fix:** Ensure Ollama is running: `ollama serve` in a terminal

### "ModelNotFound: deepseek-r1:1.5b" error
- **Fix:** Pull the model: `ollama pull deepseek-r1:1.5b`

### "LLM response was not valid JSON"
- **Cause:** Ollama model generated malformed response
- **Fix:** Try reducing temperature or using a larger model:
  ```python
  _init_ollama_llm(model_name="llama2", temperature=0.2)
  ```

### Slow response (30+ seconds)
- **Expected** for first run as Ollama loads model into memory
- **Optimize:** Use a smaller model:
  ```bash
  ollama pull mistral  # Faster, smaller
  # Then update portfolio_analysis.py
  ```

## Configuration

Edit `backend/app/portfolio_analysis.py`:

```python
# Change portfolio stocks
DEFAULT_PORTFOLIO = [
    "Your Stock 1",
    "Your Stock 2",
    ...
]

# Change LLM model (default: deepseek-r1:1.5b)
def _init_ollama_llm(model_name: str = "deepseek-r1:1.5b"):  # Change this

# Change temperature (creativity/randomness)
_init_ollama_llm(temperature=0.2)  # Lower = more focused
```

## Performance Notes

- **First request:** 20-30s (model loading)
- **Subsequent requests:** 5-15s
- **Memory required:** ~4GB RAM recommended
- **Model size:** deepseek-r1:1.5b = 1.5GB

## Models to Try

| Model | Size | Speed | Quality | Command |
|-------|------|-------|---------|---------|
| deepseek-r1:1.5b | 1.5GB | Very Fast | Good | `ollama pull deepseek-r1:1.5b` |
| mistral | 5GB | Fast | Good | `ollama pull mistral` |
| neural-chat | 4GB | Fast | Good | `ollama pull neural-chat` |
| dolphin-phi | 2GB | Very Fast | Fair | `ollama pull dolphin-phi` |
| llama2 | 4GB | Medium | Excellent | `ollama pull llama2` |

## Security Notes

- Ollama runs locally → no data sent to external servers
- API key still required if `SCREENER_ADMIN_KEY` is set
- LLM runs on your machine
