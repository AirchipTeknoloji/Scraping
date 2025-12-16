# scraper_worker/main.py - BAN TESPİTİ EKLE

import os
import sys
from datetime import datetime, date, timedelta, timezone
import logging

# Logging konfigürasyonu - STDOUT'a yaz (DigitalOcean logları görebilmek için)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)  # STDOUT'a yaz
    ]
)
logger = logging.getLogger(__name__)


class ScrapingBeeMonitor:
    """ScrapingBee ban/block tespiti"""
    
    def __init__(self):# scraper_worker/main.py - BAN TESPİTİ EKLE

import os
import sys
from datetime import datetime, date, timedelta, timezone
import logging

# Logging konfigürasyonu - STDOUT'a yaz (DigitalOcean logları görebilmek için)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout)  # STDOUT'a yaz
    ]
)
logger = logging.getLogger(__name__)


class ScrapingBeeMonitor:
    """ScrapingBee ban/block tespiti"""
    
    def __init__(self):
        self.total_requests = 0
        self.failed_requests = 0
        self.blocked_requests = 0
        self.rate_limited = 0
    
    def is_blocked_response(self, response):
        """Response blocked/banned mi kontrol et"""
        
        # Status code kontrolü
        if response.status_code in [403, 429]:  # Forbidden, Too Many Requests
            return True
        
        # ScrapingBee specific errors
        if response.status_code == 422:  # ScrapingBee error
            return True
        
        # Response body kontrolü
        try:
            data = response.json()
            
            # ScrapingBee error mesajları
            if 'error' in data:
                error_msg = str(data['error']).lower()
                if any(keyword in error_msg for keyword in ['blocked', 'banned', 'captcha', 'rate limit']):
                    return True
        except:
            pass
        
        return False
    
    def record_request(self, response):
        """Request sonucunu kaydet"""
        self.total_requests += 1
        
        if response.status_code != 200:
            self.failed_requests += 1
        
        if self.is_blocked_response(response):
            self.blocked_requests += 1
            logger.warning(f"⚠️  BLOCKED RESPONSE: Status {response.status_code}")
        
        if response.status_code == 429:
            self.rate_limited += 1
    
    def get_block_rate(self):
        """Block oranını hesapla"""
        if self.total_requests == 0:
            return 0
        return (self.blocked_requests / self.total_requests) * 100
    
    def should_alert(self):
        """Alert gönderilmeli mi?"""
        # %20'den fazla block varsa alert
        return self.get_block_rate() > 20 or self.rate_limited > 5


class ObiletScraper:
    def __init__(self, max_workers=10, max_retries=3, batch_size=500):
        # ... mevcut init kodu ...
        
        # Ban monitoring ekle
        self.ban_monitor = ScrapingBeeMonitor()
    
    def get_obilet_journeys(self, origin_id, destination_id, date_str):
        """
        Obilet JSON endpoint'inden seferleri çeker (BAN TESPİTİ İLE)
        """
        url = f"https://www.obilet.com/json/journeys/{origin_id}-{destination_id}/{date_str}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.obilet.com/'
        }
        
        try:
            response = requests.post(
                "https://app.scrapingbee.com/api/v1/",
                params={
                    "api_key": API_KEY,
                    "url": url,
                    "country_code": "tr",
                    "render_js": False,
                    "premium_proxy": True,
                },
                headers=headers,
                timeout=30
            )
            
            # ❗ BAN TESPİTİ
            self.ban_monitor.record_request(response)
            
            if response.status_code != 200:
                logger.error(f"❌ ScrapingBee error: {response.status_code} - {response.text[:200]}")
                return []
            
            data = response.json()
            journeys = data.get('journeys', [])
            
            # ... mevcut parse kodu ...
            
            return parsed_journeys
            
        except Exception as e:
            logger.error(f"❌ Request error: {e}")
            return []
    
    def send_ban_alert(self):
        """Ban/block alerti gönder"""
        from models_standalone import Notification, User
        
        session = get_session()
        
        try:
            # Admin kullanıcıları bul
            admins = session.query(User).filter_by(role='admin', is_active=True).all()
            
            for admin in admins:
                notification = Notification(
                    user_id=admin.id,
                    title="⚠️ ScrapingBee Blocking Detected!",
                    message=f"""
Scraper is experiencing high block rates:
- Total Requests: {self.ban_monitor.total_requests}
- Blocked: {self.ban_monitor.blocked_requests}
- Block Rate: {self.ban_monitor.get_block_rate():.1f}%
- Rate Limited: {self.ban_monitor.rate_limited}

Action may be required!
                    """.strip(),
                    notification_type='error',
                    priority='high',
                    is_read=False
                )
                session.add(notification)
            
            session.commit()
            logger.error(f"🚨 BAN ALERT SENT - Block rate: {self.ban_monitor.get_block_rate():.1f}%")
            
        except Exception as e:
            logger.error(f"❌ Failed to send ban alert: {e}")
        finally:
            session.close()
    
    def run(self, target_date=None, cleanup_old_data=False):
        """
        Ana scraping fonksiyonu (BAN TESPİTİ İLE)
        """
        logger.info("=" * 80)
        logger.info("🚀 Obilet Scraper Starting...")
        logger.info(f"📅 Timestamp: {datetime.utcnow().isoformat()}")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        # ... mevcut scraping kodu ...
        
        # Scraping tamamlandı - ban kontrolü
        if self.ban_monitor.should_alert():
            logger.error("🚨 HIGH BLOCK RATE DETECTED!")
            self.send_ban_alert()
        
        # Final statistics
        elapsed = time.time() - start_time
        
        logger.info("=" * 80)
        logger.info("✅ Scraper Completed!")
        logger.info(f"   Duration: {elapsed:.1f}s")
        logger.info(f"   Routes Processed: {self.completed_routes}/{self.total_routes}")
        logger.info(f"   Routes Failed: {self.failed_routes}")
        logger.info(f"   Total Journeys Scraped: {self.total_journeys}")
        logger.info("")
        logger.info("   📊 Database Changes:")
        logger.info(f"      Inserted: {total_inserted}")
        logger.info(f"      Updated: {total_updated}")
        logger.info(f"      Deleted: {total_deleted}")
        logger.info(f"      Price Changes: {total_price_changes}")
        logger.info("")
        logger.info("   🛡️  ScrapingBee Status:")
        logger.info(f"      Total Requests: {self.ban_monitor.total_requests}")
        logger.info(f"      Failed: {self.ban_monitor.failed_requests}")
        logger.info(f"      Blocked: {self.ban_monitor.blocked_requests}")
        logger.info(f"      Block Rate: {self.ban_monitor.get_block_rate():.1f}%")
        logger.info("=" * 80)
        
        return self.scraped_data


if __name__ == '__main__':
    # Database URL check
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL environment variable not set!")
        sys.exit(1)
    
    logger.info(f"✅ DATABASE_URL configured")
    logger.info(f"✅ SCRAPINGBEE_API_KEY configured: {'Yes' if os.getenv('SCRAPINGBEE_API_KEY') else 'No'}")
    
    try:
        # Scraper çalıştır
        scraper = ObiletScraper(
            max_workers=10,
            max_retries=3,
            batch_size=500
        )
        
        # Bugün için scrape et
        scraped_data = scraper.run(cleanup_old_data=True)
        
        logger.info("\n🎉 All operations completed successfully!")
        sys.exit(0)  # Success
        
    except Exception as e:
        logger.error(f"💥 FATAL ERROR: {e}", exc_info=True)
        sys.exit(1)  # Failure

        self.total_requests = 0
        self.failed_requests = 0
        self.blocked_requests = 0
        self.rate_limited = 0
    
    def is_blocked_response(self, response):
        """Response blocked/banned mi kontrol et"""
        
        # Status code kontrolü
        if response.status_code in [403, 429]:  # Forbidden, Too Many Requests
            return True
        
        # ScrapingBee specific errors
        if response.status_code == 422:  # ScrapingBee error
            return True
        
        # Response body kontrolü
        try:
            data = response.json()
            
            # ScrapingBee error mesajları
            if 'error' in data:
                error_msg = str(data['error']).lower()
                if any(keyword in error_msg for keyword in ['blocked', 'banned', 'captcha', 'rate limit']):
                    return True
        except:
            pass
        
        return False
    
    def record_request(self, response):
        """Request sonucunu kaydet"""
        self.total_requests += 1
        
        if response.status_code != 200:
            self.failed_requests += 1
        
        if self.is_blocked_response(response):
            self.blocked_requests += 1
            logger.warning(f"⚠️  BLOCKED RESPONSE: Status {response.status_code}")
        
        if response.status_code == 429:
            self.rate_limited += 1
    
    def get_block_rate(self):
        """Block oranını hesapla"""
        if self.total_requests == 0:
            return 0
        return (self.blocked_requests / self.total_requests) * 100
    
    def should_alert(self):
        """Alert gönderilmeli mi?"""
        # %20'den fazla block varsa alert
        return self.get_block_rate() > 20 or self.rate_limited > 5


class ObiletScraper:
    def __init__(self, max_workers=10, max_retries=3, batch_size=500):
        # ... mevcut init kodu ...
        
        # Ban monitoring ekle
        self.ban_monitor = ScrapingBeeMonitor()
    
    def get_obilet_journeys(self, origin_id, destination_id, date_str):
        """
        Obilet JSON endpoint'inden seferleri çeker (BAN TESPİTİ İLE)
        """
        url = f"https://www.obilet.com/json/journeys/{origin_id}-{destination_id}/{date_str}"
        
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
            'Accept': 'application/json',
            'Referer': 'https://www.obilet.com/'
        }
        
        try:
            response = requests.post(
                "https://app.scrapingbee.com/api/v1/",
                params={
                    "api_key": API_KEY,
                    "url": url,
                    "country_code": "tr",
                    "render_js": False,
                    "premium_proxy": True,
                },
                headers=headers,
                timeout=30
            )
            
            # ❗ BAN TESPİTİ
            self.ban_monitor.record_request(response)
            
            if response.status_code != 200:
                logger.error(f"❌ ScrapingBee error: {response.status_code} - {response.text[:200]}")
                return []
            
            data = response.json()
            journeys = data.get('journeys', [])
            
            # ... mevcut parse kodu ...
            
            return parsed_journeys
            
        except Exception as e:
            logger.error(f"❌ Request error: {e}")
            return []
    
    def send_ban_alert(self):
        """Ban/block alerti gönder"""
        from models_standalone import Notification, User
        
        session = get_session()
        
        try:
            # Admin kullanıcıları bul
            admins = session.query(User).filter_by(role='admin', is_active=True).all()
            
            for admin in admins:
                notification = Notification(
                    user_id=admin.id,
                    title="⚠️ ScrapingBee Blocking Detected!",
                    message=f"""
Scraper is experiencing high block rates:
- Total Requests: {self.ban_monitor.total_requests}
- Blocked: {self.ban_monitor.blocked_requests}
- Block Rate: {self.ban_monitor.get_block_rate():.1f}%
- Rate Limited: {self.ban_monitor.rate_limited}

Action may be required!
                    """.strip(),
                    notification_type='error',
                    priority='high',
                    is_read=False
                )
                session.add(notification)
            
            session.commit()
            logger.error(f"🚨 BAN ALERT SENT - Block rate: {self.ban_monitor.get_block_rate():.1f}%")
            
        except Exception as e:
            logger.error(f"❌ Failed to send ban alert: {e}")
        finally:
            session.close()
    
    def run(self, target_date=None, cleanup_old_data=False):
        """
        Ana scraping fonksiyonu (BAN TESPİTİ İLE)
        """
        logger.info("=" * 80)
        logger.info("🚀 Obilet Scraper Starting...")
        logger.info(f"📅 Timestamp: {datetime.utcnow().isoformat()}")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        # ... mevcut scraping kodu ...
        
        # Scraping tamamlandı - ban kontrolü
        if self.ban_monitor.should_alert():
            logger.error("🚨 HIGH BLOCK RATE DETECTED!")
            self.send_ban_alert()
        
        # Final statistics
        elapsed = time.time() - start_time
        
        logger.info("=" * 80)
        logger.info("✅ Scraper Completed!")
        logger.info(f"   Duration: {elapsed:.1f}s")
        logger.info(f"   Routes Processed: {self.completed_routes}/{self.total_routes}")
        logger.info(f"   Routes Failed: {self.failed_routes}")
        logger.info(f"   Total Journeys Scraped: {self.total_journeys}")
        logger.info("")
        logger.info("   📊 Database Changes:")
        logger.info(f"      Inserted: {total_inserted}")
        logger.info(f"      Updated: {total_updated}")
        logger.info(f"      Deleted: {total_deleted}")
        logger.info(f"      Price Changes: {total_price_changes}")
        logger.info("")
        logger.info("   🛡️  ScrapingBee Status:")
        logger.info(f"      Total Requests: {self.ban_monitor.total_requests}")
        logger.info(f"      Failed: {self.ban_monitor.failed_requests}")
        logger.info(f"      Blocked: {self.ban_monitor.blocked_requests}")
        logger.info(f"      Block Rate: {self.ban_monitor.get_block_rate():.1f}%")
        logger.info("=" * 80)
        
        return self.scraped_data


if __name__ == '__main__':
    # Database URL check
    DATABASE_URL = os.getenv('DATABASE_URL')
    
    if not DATABASE_URL:
        logger.error("❌ DATABASE_URL environment variable not set!")
        sys.exit(1)
    
    logger.info(f"✅ DATABASE_URL configured")
    logger.info(f"✅ SCRAPINGBEE_API_KEY configured: {'Yes' if os.getenv('SCRAPINGBEE_API_KEY') else 'No'}")
    
    try:
        # Scraper çalıştır
        scraper = ObiletScraper(
            max_workers=10,
            max_retries=3,
            batch_size=500
        )
        
        # Bugün için scrape et
        scraped_data = scraper.run(cleanup_old_data=True)
        
        logger.info("\n🎉 All operations completed successfully!")
        sys.exit(0)  # Success
        
    except Exception as e:
        logger.error(f"💥 FATAL ERROR: {e}", exc_info=True)
        sys.exit(1)  # Failure
