#!/usr/bin/env python3
"""
Melt & Repour Global Racing Scanner V4.1 (Self-Contained Edition)

A comprehensive racing data aggregator with multi-source fallback strategies,
intelligent caching, and robust error handling. All configuration is contained
within this single file for maximum portability and ease of use.

Key Features:
- Single-file design with top-level configuration
- Multi-layered fetching: httpx -> curl_cffi -> system curl -> interactive prompt
- Multiple hardened data sources with individual fallbacks
- Intelligent merging and deduplication of race data
- Professional PEP 8 formatting for maximum readability
- Integrated error reporting in HTML output
- Comprehensive timezone handling for international racing
"""

# =============================================================================
# CONFIGURATION SECTION - YOUR CONTROL PANEL
# =============================================================================

CONFIG = {
    # Application Settings
    "SCHEMA_VERSION": "4.1",
    "APP_NAME": "Melt & Repour Global Racing Scanner V4.1",
    
    # Directory Settings
    "DEFAULT_CACHE_DIR": ".cache",
    "DEFAULT_OUTPUT_DIR": "output",
    
    # HTTP Client Configuration
    "HTTP": {
        "REQUEST_TIMEOUT": 30.0,
        "MAX_CONCURRENT_REQUESTS": 12,
        "MAX_CONNECTIONS": 40,
        "MAX_KEEPALIVE_CONNECTIONS": 10,
        "MIN_HOST_INTERVAL": 0.25,
        "MAX_RETRIES": 4,
        "RETRY_BACKOFF_BASE": 2,
        "RETRY_JITTER_MAX": 0.3,
        "USER_AGENTS": [
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
            "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124 Safari/537.36",
        ]
    },
    
    # Cache Configuration
    "CACHE": {
        "DEFAULT_TTL": 1800,  # 30 minutes
        "MAX_TTL": 21600,     # 6 hours
        "MIN_TTL": 60,        # 1 minute
        "ENABLED": True
    },
    
    # Data Sources Configuration
    "SOURCES": {
        "AtTheRaces": {
            "enabled": True,
            "base_url": "https://www.attheraces.com",
            "regions": ["uk", "ireland", "usa", "france", "saf", "aus"],
            "timeout_multiplier": 1.0
        },
        "SportingLifeHorseApi": {
            "enabled": True,
            "base_url": "https://www.sportinglife.com/api/horse-racing/race",
            "timeout_multiplier": 1.2
        },
        "SkySports": {
            "enabled": True,
            "base_url": "https://www.skysports.com/racing/racecards",
            "timeout_multiplier": 1.1
        },
        "GBGreyhounds": {
            "enabled": True,
            "base_url": "https://www.sportinglife.com",
            "timeout_multiplier": 1.0
        },
        "HarnessAustralia": {
            "enabled": True,
            "base_url": "https://www.harness.org.au",
            "timeout_multiplier": 1.5
        },
        "StandardbredCanada": {
            "enabled": True,
            "base_url": "https://standardbredcanada.ca",
            "timeout_multiplier": 1.3
        }
    },
    
    # Data Filtering and Validation
    "FILTERS": {
        "MIN_FIELD_SIZE": 4,
        "MAX_FIELD_SIZE": 6,
        "MIN_RUNNERS_FOR_VALIDITY": 3,
        "MAX_RUNNERS_FOR_VALIDITY": 30,
        "MIN_FAVORITE_FRACTIONAL": 1.0,
        "MIN_SECOND_FAV_FRACTIONAL": 3.0,
        "MIN_ODDS_RATIO": 0.0
    },
    
    # Value Scoring Weights
    "SCORING": {
        "FIELD_SIZE_WEIGHT": 0.3,
        "ODDS_VALUE_WEIGHT": 0.4,
        "ODDS_SPREAD_WEIGHT": 0.2,
        "DATA_QUALITY_WEIGHT": 0.1
    },
    
    # Browser Automation (for fallback)
    "BROWSER": {
        "ENABLED": True,
        "CHROME_BINARY_PATHS": [
            "/usr/bin/google-chrome-stable",
            "/usr/bin/google-chrome",
            "/usr/bin/chromium",
            "/usr/bin/chromium-browser",
            "C:\\Program Files\\Google\\Chrome\\Application\\chrome.exe",
            "C:\\Program Files (x86)\\Google\\Chrome\\Application\\chrome.exe"
        ],
        "CHROME_OPTIONS": [
            "--headless=new",
            "--no-sandbox",
            "--disable-dev-shm-usage",
            "--disable-gpu",
            "--disable-extensions",
            "--disable-plugins"
        ]
    },
    
    # Output Configuration
    "OUTPUT": {
        "DEFAULT_FORMATS": ["html", "json"],
        "HTML_TEMPLATE": {
            "SHOW_DEBUG_INFO": False,
            "SHOW_SOURCE_BREAKDOWN": True,
            "AUTO_OPEN_BROWSER": True
        }
    },
    
    # Timezone Mappings
    "TIMEZONES": {
        "TRACKS": {
            "ayr": "Europe/London",
            "kempton-park": "Europe/London", 
            "windsor": "Europe/London",
            "ascot": "Europe/London",
            "cheltenham": "Europe/London",
            "newmarket": "Europe/London",
            "leopardstown": "Europe/Dublin",
            "curragh": "Europe/Dublin",
            "ballinrobe": "Europe/Dublin",
            "finger-lakes": "America/New_York",
            "fort-erie": "America/Toronto",
            "presque-isle-downs": "America/New_York",
            "ellis-park": "America/Chicago",
            "thistledown": "America/New_York",
            "mountaineer-park": "America/New_York",
            "mountaineer": "America/New_York",
            "churchill": "America/New_York",
            "belmont": "America/New_York",
            "saratoga": "America/New_York",
            "santa-anita": "America/Los_Angeles",
            "del-mar": "America/Los_Angeles",
            "la-teste-de-buch": "Europe/Paris",
            "clairefontaine": "Europe/Paris",
            "cagnes-sur-mer-midi": "Europe/Paris",
            "divonne-les-bains": "Europe/Paris",
            "longchamp": "Europe/Paris",
            "saint-malo": "Europe/Paris",
            "flemington": "Australia/Melbourne",
            "randwick": "Australia/Sydney",
            "eagle-farm": "Australia/Brisbane",
            "albion-park": "Australia/Brisbane",
            "redcliffe": "Australia/Brisbane",
            "menangle": "Australia/Sydney",
            "gloucester-park": "Australia/Perth",
            "fairview": "Africa/Johannesburg",
            "gavea": "America/Sao_Paulo",
            "sha-tin": "Asia/Hong_Kong",
            "tokyo": "Asia/Tokyo"
        },
        "COUNTRIES": {
            "GB": "Europe/London",
            "IE": "Europe/Dublin",
            "US": "America/New_York",
            "FR": "Europe/Paris",
            "AU": "Australia/Sydney",
            "NZ": "Pacific/Auckland",
            "HK": "Asia/Hong_Kong",
            "JP": "Asia/Tokyo",
            "ZA": "Africa/Johannesburg",
            "CA": "America/Toronto",
            "BR": "America/Sao_Paulo"
        }
    }
}

# =============================================================================
# IMPORTS AND SETUP
# =============================================================================

import asyncio
import argparse
import csv
import json
import logging
import os
import re
import hashlib
import random
import contextlib
import shutil
import subprocess
import sys
import certifi
import time
import webbrowser
from dataclasses import dataclass, asdict, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Optional, Tuple, Any, Set
from urllib.parse import urlparse, urljoin
from collections import defaultdict

try:
    from zoneinfo import ZoneInfo
except ImportError:
    from backports.zoneinfo import ZoneInfo

import httpx
import aiofiles
from bs4 import BeautifulSoup
from bs4.element import Tag
from curl_cffi.requests import Session as CurlCffiSession

# Silence urllib3 warnings when SSL verification is disabled
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)