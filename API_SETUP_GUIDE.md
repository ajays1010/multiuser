# 🚀 API Setup Guide for Stock Sentiment Analysis

This guide will help you set up the Twitter API and News API for your stock sentiment analysis system.

## 📋 Prerequisites

- Python 3.8+ installed
- Virtual environment activated
- All dependencies installed (`pip install -r requirements.txt`)

## 🐦 Twitter API Setup

### Step 1: Create Twitter Developer Account
1. Go to [Twitter Developer Portal](https://developer.twitter.com/en/portal/dashboard)
2. Sign in with your Twitter account
3. Apply for a developer account (if you don't have one)

### Step 2: Create Twitter App
1. Click "Create App" or "Create Project"
2. Fill in the required information:
   - **App name**: `StockSentimentAnalysis` (or your preferred name)
   - **Use case**: Select "Making a bot" or "Academic research"
   - **Description**: "Stock market sentiment analysis using social media data"

### Step 3: Get API Keys
1. Go to your app's "Keys and Tokens" tab
2. Copy the **Bearer Token** (this is what you need)
3. Keep your **API Key** and **API Secret** secure (not needed for this app)

### Step 4: Set Permissions
1. Go to "App permissions" tab
2. Set to "Read" (you only need to read tweets, not post)

## 📰 News API Setup

### Step 1: Create News API Account
1. Go to [NewsAPI.org](https://newsapi.org/)
2. Click "Get API Key"
3. Sign up for a free account

### Step 2: Get API Key
1. After signing up, you'll see your API key on the dashboard
2. Copy the API key

### Step 3: Free Plan Limitations
- **Free plan**: 1,000 requests per day
- **Paid plans**: Higher limits available

## 🔧 Configuration

### Option 1: Environment Variables (Recommended)
1. Copy `env_template.txt` to `.env`
2. Fill in your API keys:
```bash
TWITTER_BEARER_TOKEN=your_actual_twitter_bearer_token
NEWS_API_KEY=your_actual_news_api_key
```

### Option 2: Direct Setup
Run the setup script:
```bash
python setup_apis.py
```

### Option 3: Manual Configuration
Edit `config.py` directly:
```python
TWITTER_BEARER_TOKEN = "your_actual_twitter_bearer_token"
NEWS_API_KEY = "your_actual_news_api_key"
```

## 🧪 Testing Your Setup

### Test 1: Run Setup Script
```bash
python setup_apis.py
```

### Test 2: Test Sentiment Analyzer
```bash
python -c "from sentiment_analyzer import StockSentimentAnalyzer; print('✅ Import successful')"
```

### Test 3: Test Flask Application
```bash
python app.py
```

## 📊 What You'll Get

### With Twitter API:
- Real-time tweet sentiment analysis
- Stock-specific social media monitoring
- Engagement metrics (likes, retweets, replies)

### With News API:
- Financial news sentiment analysis
- International news coverage
- Structured news data

### With Custom Indian News Scraping:
- **Mint.com** financial news
- **Moneycontrol.com** market updates
- Indian market-specific sentiment

## 🚨 Troubleshooting

### Common Issues:

#### 1. "Twitter Bearer Token not found"
- Check if your `.env` file exists
- Verify the token is correct
- Ensure no extra spaces or quotes

#### 2. "News API key not found"
- Verify your News API key
- Check if you've exceeded daily limits
- Ensure proper environment variable format

#### 3. Import Errors
- Make sure virtual environment is activated
- Run `pip install -r requirements.txt`
- Check Python version compatibility

#### 4. Rate Limiting
- Twitter: 450 requests per 15-minute window
- News API: 1,000 requests per day (free plan)

## 🔒 Security Notes

- **Never commit API keys to version control**
- **Use environment variables for production**
- **Rotate API keys regularly**
- **Monitor API usage and costs**

## 📈 Next Steps

1. **Test the sentiment analysis** with a sample stock
2. **Monitor API usage** to stay within limits
3. **Customize news sources** if needed
4. **Scale up** with paid API plans if required

## 🆘 Support

If you encounter issues:
1. Check the error messages in your terminal
2. Verify API keys are correct
3. Ensure all dependencies are installed
4. Check API service status pages

---

**Happy Sentiment Analysis! 🎯📊**
