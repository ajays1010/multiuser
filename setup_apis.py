#!/usr/bin/env python3
"""
API Setup and Testing Script for Stock Sentiment Analysis
"""

import os
import sys
from config import TWITTER_BEARER_TOKEN, NEWS_API_KEY, INDIAN_NEWS_SOURCES

def setup_environment():
    """Setup environment variables for API keys"""
    print("🔧 Setting up API Keys for Stock Sentiment Analysis")
    print("=" * 60)
    
    # Twitter API Setup
    print("\n🐦 X.COM (TWITTER) API SETUP:")
    print("1. Go to: https://developer.x.com/en/portal/dashboard")
    print("2. Sign in with your X (Twitter) account")
    print("3. Create a new app or use existing one")
    print("4. Go to 'Keys and Tokens' tab")
    print("5. Copy your 'Bearer Token'")
    
    twitter_token = input("\nEnter your Twitter Bearer Token (or press Enter to skip): ").strip()
    if twitter_token:
        os.environ['TWITTER_BEARER_TOKEN'] = twitter_token
        print("✅ Twitter Bearer Token set")
    else:
        print("⚠️ Twitter API will not be available")
    
    # News API Setup
    print("\n📰 NEWS API SETUP:")
    print("1. Go to: https://newsapi.org/")
    print("2. Sign up for a free account")
    print("3. Get your API key from the dashboard")
    
    news_key = input("\nEnter your News API key (or press Enter to skip): ").strip()
    if news_key:
        os.environ['NEWS_API_KEY'] = news_key
        print("✅ News API key set")
    else:
        print("⚠️ News API will not be available")
    
    print("\n" + "=" * 60)
    return twitter_token, news_key

def test_sentiment_analyzer():
    """Test the sentiment analyzer with sample data"""
    print("\n🧪 TESTING SENTIMENT ANALYZER:")
    print("=" * 60)
    
    try:
        from sentiment_analyzer import StockSentimentAnalyzer
        
        # Test with sample stock
        test_stock = "RELIANCE"
        test_company = "Reliance Industries"
        
        print(f"Testing with stock: {test_stock} ({test_company})")
        
        analyzer = StockSentimentAnalyzer()
        
        # Test sentiment analysis on sample text
        sample_texts = [
            "Reliance Industries reports strong quarterly results with 25% growth",
            "Reliance stock falls due to market concerns",
            "Reliance announces new digital initiatives"
        ]
        
        print("\n📊 Testing sentiment analysis on sample texts:")
        for text in sample_texts:
            sentiment_score, sentiment_label = analyzer.analyze_sentiment(text)
            print(f"   Text: {text[:50]}...")
            print(f"   Sentiment: {sentiment_label} (Score: {sentiment_score:.3f})")
            print()
        
        print("✅ Sentiment analyzer is working correctly!")
        
    except ImportError as e:
        print(f"❌ Error importing sentiment analyzer: {e}")
        print("Make sure all dependencies are installed")
    except Exception as e:
        print(f"❌ Error testing sentiment analyzer: {e}")

def create_env_file():
    """Create a .env file template"""
    print("\n📝 CREATING ENVIRONMENT FILE:")
    print("=" * 60)
    
    env_content = """# Stock Sentiment Analysis - Environment Variables
# Copy this file to .env and fill in your actual API keys

# Twitter API Configuration
TWITTER_BEARER_TOKEN=your_twitter_bearer_token_here

# News API Configuration  
NEWS_API_KEY=your_news_api_key_here

# Supabase Configuration (if needed)
SUPABASE_URL=your_supabase_url_here
SUPABASE_SERVICE_KEY=your_supabase_service_key_here

# Telegram Bot Configuration
TELEGRAM_BOT_TOKEN=
"""
    
    try:
        with open('env_template.txt', 'w') as f:
            f.write(env_content)
        print("✅ Created env_template.txt file")
        print("📋 Copy this to .env and fill in your actual API keys")
    except Exception as e:
        print(f"❌ Error creating env file: {e}")

def main():
    """Main setup function"""
    print("🚀 STOCK SENTIMENT ANALYSIS - API SETUP")
    print("=" * 60)
    
    # Setup environment variables
    twitter_token, news_key = setup_environment()
    
    # Test the sentiment analyzer
    test_sentiment_analyzer()
    
    # Create environment file template
    create_env_file()
    
    print("\n🎯 NEXT STEPS:")
    print("1. Copy env_template.txt to .env")
    print("2. Fill in your actual API keys in .env")
    print("3. Restart your Flask application")
    print("4. Test sentiment analysis in the web interface")
    
    if twitter_token or news_key:
        print("\n✅ Your application is ready to use with the configured APIs!")
    else:
        print("\n⚠️ No APIs configured. The sentiment analysis will work with limited functionality.")

if __name__ == "__main__":
    main()
