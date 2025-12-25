# scraper_worker/main.py (FULL VERSION)
import requests
import json
from datetime import datetime, date, timedelta, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock
import logging
import os
import time


import sys


# Models import
from models_standalone import (
    Route, Journey, PriceHistory, PriceAlert, 
    CompanyRoute, User, get_session
)

# Logging setup
#logging.basicConfig(
#    level=logging.INFO,
#    format='%(asctime)s - %(levelname)s - %(message)s'
#)
#logger = logging.getLogger(__name__)

# ScrapingBee API Key
API_KEY = os.getenv('SCRAPINGBEE_API_KEY', '')

# Telegram Config
TELEGRAM_BOT_TOKEN = os.getenv('TELEGRAM_BOT_TOKEN', '')
TELEGRAM_CHAT_ID = os.getenv('TELEGRAM_CHAT_ID', '')



def send_telegram_message(message: str, chat_id: str = None, parse_mode: str = 'HTML') -> bool:
    """
    Telegram üzerinden mesaj gönder
    chat_id belirtilmezse varsayılan TELEGRAM_CHAT_ID kullanılır (admin bildirimleri için)
    """
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("⚠️ Telegram bot token not configured")
        return False
    
    target_chat_id = chat_id or TELEGRAM_CHAT_ID
    
    if not target_chat_id:
        logger.warning("⚠️ No chat_id provided and TELEGRAM_CHAT_ID not configured")
        return False
    
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
        payload = {
            'chat_id': target_chat_id,
            'text': message,
            'parse_mode': parse_mode
        }
        
        response = requests.post(url, json=payload, timeout=10)
        
        if response.status_code == 200:
            logger.info(f"✅ Telegram message sent to {target_chat_id}")
            return True
        else:
            logger.error(f"❌ Telegram API error: {response.status_code} - {response.text}")
            return False
            
    except Exception as e:
        logger.error(f"❌ Failed to send Telegram message: {e}")
        return False


def send_price_alert_telegram(user, route_name: str, change: dict) -> bool:
    """
    Kullanıcıya fiyat değişikliği Telegram bildirimi gönder
    """
    # Kullanıcının telegram_id'si yoksa gönderme
    if not user.telegram_id:
        return False
    
    journey = change['journey']
    old_price = change['old_price']
    new_price = change['new_price']
    change_pct = change['change_pct']
    
    # Emoji ve mesaj türü
    if change_pct < 0:
        emoji = "📉"
        title = "Fiyat Düştü!"
    else:
        emoji = "📈"
        title = "Fiyat Arttı!"
    
    departure_time = journey.departure_time.strftime('%H:%M') if journey.departure_time else 'N/A'
    departure_date = journey.departure_time.strftime('%d.%m.%Y') if journey.departure_time else 'N/A'
    
    message = f"""
{emoji} <b>{title}</b>

🚌 <b>Firma:</b> {journey.company_name}
🛣 <b>Güzergah:</b> {route_name}
📅 <b>Tarih:</b> {departure_date}
🕐 <b>Saat:</b> {departure_time}

💰 <b>Eski Fiyat:</b> {old_price:.2f} TRY
💰 <b>Yeni Fiyat:</b> {new_price:.2f} TRY
📊 <b>Değişim:</b> {change_pct:+.1f}%
""".strip()
    
    return send_telegram_message(message, chat_id=str(user.telegram_id))


def send_new_journey_telegram(user, route_name: str, journey, is_lowest_price: bool = False) -> bool:
    """
    Kullanıcıya yeni sefer Telegram bildirimi gönder
    """
    if not user.telegram_id:
        return False
    
    departure_time = journey.departure_time.strftime('%H:%M') if journey.departure_time else 'N/A'
    departure_date = journey.departure_time.strftime('%d.%m.%Y') if journey.departure_time else 'N/A'
    
    lowest_badge = "\n🏆 <b>EN DÜŞÜK FİYAT!</b>" if is_lowest_price else ""
    
    message = f"""
🆕 <b>Yeni Sefer Eklendi!</b>

🚌 <b>Firma:</b> {journey.company_name}
🛣 <b>Güzergah:</b> {route_name}
📅 <b>Tarih:</b> {departure_date}
🕐 <b>Saat:</b> {departure_time}
💰 <b>Fiyat:</b> {journey.internet_price:.2f} TRY{lowest_badge}
""".strip()
    
    return send_telegram_message(message, chat_id=str(user.telegram_id))


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
    def __init__(self, max_workers=5, max_retries=10, batch_size=500):
        self.max_workers = max_workers
        self.max_retries = max_retries
        self.batch_size = batch_size
        
        # Buffers - memory'de biriktir
        self.scraped_data = []  # Tüm scrape edilen journeys
        self.lock = Lock()
        
        # Statistics
        self.total_routes = 0
        self.completed_routes = 0
        self.failed_routes = 0
        self.failed_routes_list = []  # Başarısız rotaların listesi
        self.total_journeys = 0
        self.ban_monitor = ScrapingBeeMonitor()
        
    def get_active_routes(self):
        """Database'den aktif route'ları çek"""
        session = get_session()
        try:
            routes = session.query(Route).filter_by(is_active=True).all()
            logger.info(f"📋 Found {len(routes)} active routes in database")
            return routes
        except Exception as e:
            logger.error(f"❌ Database error: {e}")
            return []
        finally:
            session.close()
    
    def get_obilet_journeys(self, origin_id, destination_id, date_str):
        """
        Obilet JSON endpoint'inden seferleri çeker (ScrapingBee ile)
        """
        url = f"https://www.obilet.com/json/journeys/{origin_id}-{destination_id}/{date_str}"
        print(url)
        
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
                    "premium_proxy": False,
                    "forward_headers":True,
                    "timeout": 60000,  # 60 saniye (büyük JSON'lar için)
                },
                headers=headers,
                timeout=70  # Request timeout (ScrapingBee timeout'undan biraz fazla)
            )

            self.ban_monitor.record_request(response)

            if response.status_code != 200:
                logger.error(f"❌ ScrapingBee error: {response.status_code}")
                return None  # ❌ API hatası - None döndür
            
            data = response.json()
            journeys = data.get('journeys', [])
            
            parsed_journeys = []
            
            for j in journeys:
                journey = j.get('journey', {})
                
                parsed = {
                    'id': j.get('id'),
                    'partner_id': j.get('partner-id'),
                    'partner_name': j.get('partner-name'),
                    'bus_type': j.get('bus-type'),
                    'total_seats': j.get('total-seats'),
                    'available_seats': j.get('available-seats'),
                    
                    # Journey detayları
                    'origin': journey.get('origin'),
                    'destination': journey.get('destination'),
                    'departure': journey.get('departure'),
                    'arrival': journey.get('arrival'),
                    'duration': 0,
                    
                    # Fiyat
                    'original_price': journey.get('original-price'),
                    'internet_price': journey.get('internet-price'),
                    'currency': journey.get('currency'),
                    
                    # Diğer bilgiler
                    'bus_name': journey.get('bus-name'),
                    'peron_no': journey.get('peron-no'),
                    
                    # Özellikler
                    'features': [],
                    
                    # Duraklar
                    'stops': [
                        {
                            'name': stop.get('name'),
                            'time': stop.get('time'),
                            'is_origin': stop.get('is-origin'),
                            'is_destination': stop.get('is-destination')
                        }
                        for stop in journey.get('stops', [])
                    ],
                    
                    # Rating
                    'partner_rating': j.get('partner-rating'),
                    'partner_route_rating': j.get('partner-route-rating'),
                }
                
                parsed_journeys.append(parsed)
            
            # ✅ API başarılı - boş liste bile olsa liste döndür
            return parsed_journeys
            
        except requests.exceptions.RequestException as e:
            logger.error(f"❌ Request error: {e}")
            return None  # ❌ Network hatası
        except json.JSONDecodeError as e:
            logger.error(f"❌ JSON parse error: {e}")
            return None  # ❌ Parse hatası
        except Exception as e:
            logger.error(f"❌ Unexpected error: {e}")
            return None  # ❌ Beklenmeyen hata


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


    def buffer_journeys(self, route, journeys, date_str):
        """Thread-safe buffer'a ekle"""
        with self.lock:
            for journey in journeys:
                # Route bilgisini de ekle
                journey['route_id'] = route.id
                journey['route_name'] = route.route_name or f"{route.origin_city_name} - {route.destination_city_name}"
                journey['scraped_date'] = date_str
                journey['scraped_at'] = datetime.utcnow().isoformat()
                
                self.scraped_data.append(journey)
            
            self.total_journeys += len(journeys)
    
    def parse_datetime_safe(self, datetime_str):
        """
        Datetime string'i parse et, timezone ekle
        """
        if not datetime_str:
            return None
        
        try:
            # ISO format parse et
            dt = datetime.fromisoformat(datetime_str.replace('Z', '+00:00'))
            
            # Timezone yoksa UTC ekle
            if dt.tzinfo is None:
                from datetime import timezone
                dt = dt.replace(tzinfo=timezone.utc)
            
            return dt
        except:
            try:
                # Alternatif format
                dt = datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S')
                from datetime import timezone
                dt = dt.replace(tzinfo=timezone.utc)
                return dt
            except:
                logger.warning(f"⚠️  Could not parse datetime: {datetime_str}")
                return None

    def filter_journeys_by_date(self, journeys, target_date):
        """
        Sadece target_date'e ait journey'leri filtrele
        Ertesi günün seferlerini exclude et
        """
        filtered = []
        excluded_count = 0
        
        for journey in journeys:
            departure_str = journey.get('departure')
            
            if not departure_str:
                continue
            
            # Parse et
            departure_dt = self.parse_datetime_safe(departure_str)
            
            if not departure_dt:
                continue
            
            # Tarihi karşılaştır (sadece date kısmı)
            journey_date = departure_dt.date()
            
            if journey_date == target_date:
                filtered.append(journey)
            else:
                excluded_count += 1
                logger.debug(f"  ⏭️  Excluded: {journey.get('partner_name')} @ {departure_dt} (different date)")
        
        if excluded_count > 0:
            logger.info(f"  📅 Filtered: {len(filtered)} kept, {excluded_count} excluded (wrong date)")
        
        return filtered

    def scrape_route_with_retry(self, route, date_str):
        """
        Tek bir route için scraping yap (GÜNCELLENMİŞ)
        """
        route_name = route.route_name or f"{route.origin_city_name} → {route.destination_city_name}"
        logger.warning(f"⚠️  {route_name}: TEKRAR DENENİYOR!!!!!!!")
        
        for attempt in range(self.max_retries):
            try:
                # Obilet'ten veri çek
                journeys = self.get_obilet_journeys(
                    origin_id=route.origin_obilet_id,
                    destination_id=route.destination_obilet_id,
                    date_str=date_str
                )
                
                # ❌ API hatası (None döndü) - retry yapılacak
                if journeys is None:
                    if attempt == self.max_retries - 1:
                        logger.error(f"❌ {route_name}: API failed after {self.max_retries} attempts")
                        with self.lock:
                            self.failed_routes += 1
                            self.failed_routes_list.append(route_name)  # Başarısız rota listesine ekle
                        return {'success': False, 'api_error': True}
                    else:
                        logger.warning(f"⚠️  {route_name}: API error, retrying... (attempt {attempt + 1}/{self.max_retries})")
                        time.sleep(2 ** attempt)  # Exponential backoff
                        continue
                
                # ✅ API başarılı (boş liste de olabilir)
                if journeys:
                    # Target date objesini oluştur
                    target_date = datetime.strptime(date_str, '%Y-%m-%d').date()
                    
                    # ❗ SADECE O GÜNÜN SEFERLERİNİ FİLTRELE
                    filtered_journeys = self.filter_journeys_by_date(journeys, target_date)
                    
                    if filtered_journeys:
                        # Buffer'a ekle
                        self.buffer_journeys(route, filtered_journeys, date_str)
                        
                        with self.lock:
                            self.completed_routes += 1
                        
                        logger.info(f"✅ [{self.completed_routes}/{self.total_routes}] {route_name}: {len(filtered_journeys)} journeys (filtered from {len(journeys)})")
                        return {'success': True, 'count': len(filtered_journeys)}
                    else:
                        logger.warning(f"⚠️  {route_name}: All journeys excluded (wrong date)")
                        with self.lock:
                            self.completed_routes += 1
                        return {'success': True, 'count': 0}
                else:
                    # ✅ API başarılı ama boş liste (sefer yok)
                    logger.warning(f"⚠️  {route_name}: No journeys found (API returned empty)")
                    with self.lock:
                        self.completed_routes += 1
                    return {'success': True, 'count': 0}
                
            except Exception as e:
                if attempt == self.max_retries - 1:
                    logger.error(f"❌ {route_name} failed after {self.max_retries} attempts: {e}")
                    with self.lock:
                        self.failed_routes += 1
                    return {'success': False, 'error': str(e)}
                
                wait_time = 2 ** attempt
                logger.warning(f"⟳ {route_name} attempt {attempt + 1}/{self.max_retries} failed, retrying in {wait_time}s...")
                time.sleep(wait_time)

    def get_unique_key(self, journey_data):
        """
        Journey'yi benzersiz şekilde tanımlayan key
        (route_id, departure_time_iso, partner_id)
        """
        # Departure'ı parse et ve isoformat'a çevir (DB ile eşleşmesi için)
        departure_dt = self.parse_datetime_safe(journey_data.get('departure'))
        departure_iso = departure_dt.isoformat() if departure_dt else None
        
        return (
            journey_data['route_id'],
            departure_iso,
            journey_data.get('partner_id')
        )
    

    def create_journey_object(self, data):
        """
        Scraped data'dan Journey objesi oluştur (GÜNCELLENMİŞ)
        """
        # ❗ YENİ PARSE FONKSİYONU KULLAN
        departure_dt = self.parse_datetime_safe(data.get('departure'))
        arrival_dt = self.parse_datetime_safe(data.get('arrival'))
        
        # Occupancy rate hesapla
        occupancy_rate = None
        if data.get('total_seats') and data['total_seats'] > 0:
            occupied = data['total_seats'] - data.get('available_seats', 0)
            occupancy_rate = round((occupied / data['total_seats']) * 100, 2)
        
        return Journey(
            route_id=data['route_id'],
            company_name=data.get('partner_name', 'Unknown'),
            obilet_partner_id=data.get('partner_id'),
            departure_time=departure_dt,
            arrival_time=arrival_dt,
            duration=data.get('duration'),
            original_price=data.get('original_price'),
            internet_price=data.get('internet_price'),
            currency=data.get('currency', 'TRY'),
            total_seats=data.get('total_seats'),
            available_seats=data.get('available_seats', 0),
            occupancy_rate=occupancy_rate,
            bus_type=data.get('bus_type'),
            bus_plate="",
            #bus_plate=data.get('bus_name'),
            has_wifi='Wifi' in data.get('features', []) or 'Wi-Fi' in data.get('features', []),
            has_usb='USB' in data.get('features', []),
            has_tv='TV' in data.get('features', []) or 'Ekran' in data.get('features', []),
            has_socket='Priz' in data.get('features', []) or 'Şarj' in data.get('features', []),
            obilet_journey_id=data.get('id'),
            is_active=True
        )

    def sync_journeys_for_route(self, route_id, new_journeys_data, target_date):
        """
        Bir route için journey'leri senkronize et - HARD DELETE
        - API'den gelen güncel data ile DB'deki journeys'leri karşılaştır
        - API'de olmayan herkesi GERÇEKTEN SİL (hard delete)
        - is_active kullanmıyoruz artık
        """
        session = get_session()
        
        try:
            # 1. DB'den o route için TÜM journeys'i çek (is_active yok artık)
            existing_journeys = session.query(Journey).filter(
                Journey.route_id == route_id
            ).all()
            
            # Existing journeys'i obilet_journey_id'ye göre dict'e çevir
            existing_dict = {}
            for j in existing_journeys:
                if j.obilet_journey_id:
                    existing_dict[str(j.obilet_journey_id)] = j
            
            # New journeys'i obilet_journey_id'ye göre dict'e çevir
            new_dict = {}
            for data in new_journeys_data:
                journey_id = data.get('id')
                if journey_id:
                    new_dict[str(journey_id)] = data
            
            existing_ids = set(existing_dict.keys())
            new_ids = set(new_dict.keys())
            
            logger.info(f"  🔍 Debug: Existing IDs count: {len(existing_ids)}, New IDs count: {len(new_ids)}")
            
            # 2. Silinecekler - API'de olmayan herkesi GERÇEKTEN SİL
            to_delete_ids = existing_ids - new_ids
            deleted_count = 0
            
            if to_delete_ids:
                logger.info(f"  🗑️  Will DELETE {len(to_delete_ids)} journeys (hard delete)")
            
            for journey_id in to_delete_ids:
                journey = existing_dict[journey_id]
                session.delete(journey)  # 🗑️ HARD DELETE
                deleted_count += 1
                logger.info(f"  🗑️  Deleted: {journey.company_name} @ {journey.departure_time.strftime('%Y-%m-%d %H:%M') if journey.departure_time else 'N/A'} (ID: {journey_id})")
            
            # 3. Güncellenecekler
            to_update_ids = existing_ids & new_ids
            updated_count = 0
            price_changes = []
            
            for journey_id in to_update_ids:
                existing_journey = existing_dict[journey_id]
                new_data = new_dict[journey_id]
                
                new_price = new_data.get('internet_price')
                new_seats = new_data.get('available_seats', 0)
                
                old_price = existing_journey.internet_price
                
                # 🔧 Float/Decimal sorunu - hepsini float yap
                if old_price is not None:
                    old_price = float(old_price)
                if new_price is not None:
                    new_price = float(new_price)
                
                price_changed = old_price and new_price and old_price != new_price
                seats_changed = existing_journey.available_seats != new_seats
                
                if price_changed or seats_changed:
                    existing_journey.internet_price = new_price
                    existing_journey.original_price = new_data.get('original_price')
                    existing_journey.available_seats = new_seats
                    existing_journey.total_seats = new_data.get('total_seats')
                    
                    if new_data.get('total_seats') and new_data['total_seats'] > 0:
                        occupied = new_data['total_seats'] - new_seats
                        existing_journey.occupancy_rate = round((occupied / new_data['total_seats']) * 100, 2)
                    
                    existing_journey.scraped_at = datetime.utcnow()
                    updated_count += 1
                    
                    if price_changed:
                        change_pct = ((new_price - old_price) / old_price) * 100
                        price_changes.append({
                            'journey': existing_journey,
                            'old_price': old_price,
                            'new_price': new_price,
                            'change_pct': change_pct
                        })
                        logger.info(f"  💰 Price changed: {existing_journey.company_name} @ {existing_journey.departure_time.strftime('%H:%M') if existing_journey.departure_time else 'N/A'} | {old_price} → {new_price} TRY ({change_pct:+.1f}%)")
            
            # 4. Eklenecekler - Gerçekten yeni olanları ekle
            to_insert_ids = new_ids - existing_ids
            inserted_journeys = []
            
            if to_insert_ids:
                logger.info(f"  ➕ Will insert {len(to_insert_ids)} new journeys")
            
            for journey_id in to_insert_ids:
                new_data = new_dict[journey_id]
                journey_obj = self.create_journey_object(new_data)
                session.add(journey_obj)
                inserted_journeys.append(journey_obj)
                logger.info(f"  ➕ New journey: {journey_obj.company_name} @ {journey_obj.departure_time.strftime('%H:%M') if journey_obj.departure_time else 'N/A'} | {journey_obj.internet_price} TRY (ID: {journey_id})")
            
            session.commit()
            
            # 5. Alert oluştur
            # Eğer DB'de hiç journey yoktuysa = günün ilk dolumu = yeni sefer bildirimi gönderme
            is_first_run = len(existing_ids) == 0
            
            if is_first_run:
                logger.info(f"  ℹ️  First run for route {route_id} - skipping new journey notifications")
            
            self.create_alerts_for_changes(
                session=session,
                route_id=route_id,
                price_changes=price_changes,
                new_journeys=inserted_journeys,
                target_date=target_date,
                skip_new_journey_alerts=is_first_run  # İlk dolumda yeni sefer bildirimi gönderme
            )
            
            logger.info(f"  📊 Route {route_id} sync: {len(to_insert_ids)} inserted, {updated_count} updated, {deleted_count} deleted")
            
            return {
                'inserted': len(to_insert_ids),
                'updated': updated_count,
                'deleted': deleted_count,
                'price_changes': len(price_changes)
            }
            
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Sync error for route {route_id}: {e}")
            raise
        finally:
            session.close()
        
    def create_alerts_for_changes(self, session, route_id, price_changes, new_journeys, target_date, skip_new_journey_alerts=False):
        """
        Fiyat değişiklikleri ve yeni seferler için:
        - PriceAlert tablosuna kaydet
        - Notification tablosuna kaydet
        - Telegram bildirimi gönder
        
        skip_new_journey_alerts: True ise yeni sefer bildirimi gönderilmez (günün ilk dolumu için)
        """
        from models_standalone import Notification
        
        try:
            # Route bilgisini al
            route = session.query(Route).filter(Route.id == route_id).first()
            route_name = route.route_name if route else f"Route {route_id}"
            
            # Bu route'u takip eden firmaları bul
            company_routes = session.query(CompanyRoute).filter(
                CompanyRoute.route_id == route_id,
                CompanyRoute.is_active == True
            ).all()
            
            if not company_routes:
                return
            
            # Her firma için alert oluştur
            for cr in company_routes:
                user = cr.user
                
                # Fiyat değişikliği alertleri - KONTROL YOK, her değişiklikte bildirim
                for change in price_changes:
                    alert_type = 'price_drop' if change['change_pct'] < 0 else 'price_increase'
                    emoji = '📉' if change['change_pct'] < 0 else '📈'
                    title = f"{'Fiyat Düştü' if alert_type == 'price_drop' else 'Fiyat Arttı'}: {change['journey'].company_name}"
                    message = f"{change['journey'].company_name} firmasının {change['journey'].departure_time.strftime('%H:%M') if change['journey'].departure_time else 'N/A'} seferinde fiyat {change['old_price']:.2f} TRY'den {change['new_price']:.2f} TRY'ye değişti ({change['change_pct']:+.1f}%)"
                    
                    # 1. PriceAlert tablosuna kaydet
                    alert = PriceAlert(
                        user_id=user.id,
                        route_id=route_id,
                        alert_type=alert_type,
                        title=title,
                        message=message,
                        competitor_name=change['journey'].company_name,
                        old_price=change['old_price'],
                        new_price=change['new_price'],
                        price_change_percentage=change['change_pct'],
                        departure_date=target_date,
                        priority='high' if abs(change['change_pct']) > 20 else 'medium',
                        is_read=False,
                        is_sent=False
                    )
                    session.add(alert)
                    
                    # 2. Notification tablosuna kaydet
                    notification = Notification(
                        user_id=user.id,
                        title=f"{emoji} {title}",
                        message=message,
                        notification_type='price_change',
                        priority='high' if abs(change['change_pct']) > 20 else 'medium',
                        is_read=False
                    )
                    session.add(notification)
                    
                    logger.info(f"    🔔 Alert + Notification created for {user.company_name}: Price change")
                    
                    # 3. Telegram Bildirimi Gönder (telegram_id varsa)
                    if user.telegram_id:
                        send_price_alert_telegram(user, route_name, change)
                        logger.info(f"    📱 Telegram sent to {user.company_name}")
                
                # Yeni sefer alertleri - Günün ilk dolumunda GÖNDERME
                if skip_new_journey_alerts:
                    continue  # Bu kullanıcı için yeni sefer bildirimi atla
                
                for new_journey in new_journeys:
                    # En düşük fiyatlı mı kontrol et
                    min_price_journey = session.query(Journey).filter(
                        Journey.route_id == route_id,
                        Journey.departure_time >= datetime.combine(target_date, datetime.min.time()),
                        Journey.departure_time < datetime.combine(target_date + timedelta(days=1), datetime.min.time())
                    ).order_by(Journey.internet_price.asc()).first()
                    
                    is_lowest_price = (min_price_journey and 
                                      new_journey.internet_price == min_price_journey.internet_price)
                    
                    title = f"Yeni Sefer Eklendi: {new_journey.company_name}"
                    message = f"{new_journey.company_name} firması {new_journey.departure_time.strftime('%H:%M') if new_journey.departure_time else 'N/A'} seferini ekledi. Fiyat: {new_journey.internet_price} TRY" + (" - EN DÜŞÜK FİYAT! 🎉" if is_lowest_price else "")
                    
                    # 1. PriceAlert tablosuna kaydet
                    alert = PriceAlert(
                        user_id=user.id,
                        route_id=route_id,
                        alert_type='new_journey',
                        title=title,
                        message=message,
                        competitor_name=new_journey.company_name,
                        new_price=new_journey.internet_price,
                        departure_date=target_date,
                        priority='high' if is_lowest_price else 'low',
                        is_read=False,
                        is_sent=False
                    )
                    session.add(alert)
                    
                    # 2. Notification tablosuna kaydet
                    notification = Notification(
                        user_id=user.id,
                        title=f"🆕 {title}",
                        message=message,
                        notification_type='new_journey',
                        priority='high' if is_lowest_price else 'low',
                        is_read=False
                    )
                    session.add(notification)
                    
                    logger.info(f"    🔔 Alert + Notification created for {user.company_name}: New journey")
                    
                    # 3. Telegram Bildirimi Gönder (telegram_id varsa)
                    if user.telegram_id:
                        send_new_journey_telegram(user, route_name, new_journey, is_lowest_price)
                        logger.info(f"    📱 Telegram sent to {user.company_name}: New journey")
            
            session.commit()
            
        except Exception as e:
            logger.error(f"❌ Alert creation error: {e}")
    
    def insert_price_history_for_route(self, route_journeys, target_date):
        """
        Bir route için Price History ekle
        """
        session = get_session()
        
        try:
            price_records = []
            
            for data in route_journeys:
                departure_dt = None
                if data.get('departure'):
                    try:
                        departure_dt = datetime.fromisoformat(data['departure'].replace('Z', '+00:00'))
                    except:
                        pass
                
                occupancy_rate = None
                if data.get('total_seats') and data['total_seats'] > 0:
                    occupied = data['total_seats'] - data.get('available_seats', 0)
                    occupancy_rate = round((occupied / data['total_seats']) * 100, 2)
                
                days_before = (target_date - date.today()).days if target_date else 0
                
                price_hist = PriceHistory(
                    route_id=data['route_id'],
                    company_name=data.get('partner_name', 'Unknown'),
                    obilet_partner_id=data.get('partner_id'),
                    price=data.get('internet_price'),
                    currency=data.get('currency', 'TRY'),
                    departure_date=target_date,
                    days_before_departure=days_before,
                    available_seats=data.get('available_seats', 0),
                    total_seats=data.get('total_seats'),
                    occupancy_rate=occupancy_rate
                )
                
                price_records.append(price_hist)
            
            if price_records:
                session.bulk_save_objects(price_records)
                session.commit()
                logger.info(f"    💾 Price History: {len(price_records)} records added")
            
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Price History error: {e}")
        finally:
            session.close()
    
    def cleanup_old_data(self, days_to_keep=0):
        """
        Eski verileri temizle
        - Journey: is_active=False ve eski olanları sil
        - PriceHistory: X günden eski olanları sil
        """
        session = get_session()
        cutoff_date = datetime.utcnow() - timedelta(days=days_to_keep)
        
        try:
            # Journey'leri temizle (soft deleted + eski)
            deleted_journeys = session.query(Journey).filter(
                Journey.is_active == False,
                Journey.scraped_at < cutoff_date
            ).delete()
            
            # Price History temizle
            deleted_price_history = session.query(PriceHistory).filter(
                PriceHistory.recorded_at < cutoff_date
            ).delete()
            
            session.commit()
            
            logger.info(f"🧹 Cleanup: {deleted_journeys} old journeys, {deleted_price_history} old price records deleted")
            
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Cleanup error: {e}")
        finally:
            session.close()
    
    def cleanup_past_journeys(self, target_date):
        """
        Geçmiş günlere ait TÜM journey'leri sil
        Bu sayede günün ilk scrape'inde DB boş olur ve 
        yeni sefer bildirimi gönderilmez
        """
        session = get_session()
        
        try:
            # Bugünün başlangıcı (00:00:00)
            today_start = datetime.combine(target_date, datetime.min.time())
            
            # Bugünden önceki TÜM journey'leri sil (HARD DELETE)
            deleted_count = session.query(Journey).filter(
                Journey.departure_time < today_start
            ).delete()
            
            session.commit()
            
            if deleted_count > 0:
                logger.info(f"🗑️  Deleted {deleted_count} past journeys (before {target_date})")
            else:
                logger.info(f"✅ No past journeys to delete")
            
        except Exception as e:
            session.rollback()
            logger.error(f"❌ Cleanup past journeys error: {e}")
        finally:
            session.close()
    
    def run(self, target_date=None, cleanup_old_data=False):
        """
        Ana scraping + sync fonksiyonu
        """
        logger.info("=" * 80)
        logger.info("🚀 Obilet Scraper Starting...")
        logger.info("=" * 80)
        
        start_time = time.time()
        
        # Target date (default: bugün)
        if not target_date:
            target_date = date.today()
        
        date_str = target_date.strftime('%Y-%m-%d')
        logger.info(f"📅 Target Date: {date_str}")
        
        # Eski verileri temizle (opsiyonel)
        if cleanup_old_data:
            logger.info("\n🧹 Cleaning up old data...")
            self.cleanup_old_data(days_to_keep=30)
            logger.info("")
        
        # 🗑️ Geçmiş günlere ait TÜM journey'leri sil (günün ilk dolumu için)
        self.cleanup_past_journeys(target_date)
        
        # Database'den route'ları çek
        routes = self.get_active_routes()
        
        if not routes:
            logger.error("❌ No active routes found in database!")
            return
        
        self.total_routes = len(routes)
        logger.info(f"📊 Total Routes: {self.total_routes}")
        logger.info(f"⚙️  Max Workers: {self.max_workers}")
        logger.info("-" * 80)
        
        # Statistics
        total_inserted = 0
        total_updated = 0
        total_deleted = 0
        total_price_changes = 0
        
        # Her route için scrape et
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            # Future'ları dictionary'de tut
            future_to_route = {
                executor.submit(self.scrape_route_with_retry, route, date_str): route
                for route in routes
            }
            
            for future in as_completed(future_to_route):
                route = future_to_route[future]
                
                try:
                    # Scraping sonucu
                    result = future.result()
                    
                    # ✅ API başarılı - boş liste de olabilir
                    if result['success']:
                        # Bu route için scraped journeys'i al (boş liste olabilir)
                        route_journeys = [
                            j for j in self.scraped_data 
                            if j['route_id'] == route.id
                        ]
                        
                        # Sync yap - API başarılıysa boş bile olsa sync et
                        logger.info(f"\n🔄 Syncing route {route.id}: {route.route_name or 'N/A'} ({len(route_journeys)} journeys)")
                        sync_result = self.sync_journeys_for_route(
                            route_id=route.id,
                            new_journeys_data=route_journeys,
                            target_date=target_date
                        )
                        
                        total_inserted += sync_result['inserted']
                        total_updated += sync_result['updated']
                        total_deleted += sync_result['deleted']
                        total_price_changes += sync_result['price_changes']
                        
                        # Price History ekle (sadece veri varsa)
                        if route_journeys:
                            self.insert_price_history_for_route(route_journeys, target_date)
                    else:
                        # ❌ API hatası - eski verileri koru (sync yapma)
                        logger.warning(f"⚠️  Route {route.id} skipped sync (API error - preserving old data)")
                    
                except Exception as e:
                    logger.error(f"❌ Error processing route {route.id}: {e}")
        
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
        logger.info("=" * 80)
        
        # 📱 Telegram Bildirimi Gönder
        status_emoji = "✅" if self.failed_routes == 0 else "⚠️"
        
        # Tarih ve saat bilgisi
        now = datetime.now()
        datetime_str = now.strftime('%Y-%m-%d %H:%M')
        
        telegram_message = f"""
{status_emoji} <b>Scraper Tamamlandı</b>

📅 <b>Tarih:</b> {datetime_str}
🎯 <b>Hedef:</b> {target_date.strftime('%Y-%m-%d')}
⏱ <b>Süre:</b> {elapsed:.1f}s

📊 <b>Route İstatistikleri:</b>
• İşlenen: {self.completed_routes}/{self.total_routes}
• Başarısız: {self.failed_routes}
• Toplam Journey: {self.total_journeys}

💾 <b>Database Değişiklikleri:</b>
• Eklenen: {total_inserted}
• Güncellenen: {total_updated}
• Silinen: {total_deleted}
• Fiyat Değişimi: {total_price_changes}
""".strip()
        
        # Başarısız rotaları ekle
        if self.failed_routes_list:
            failed_routes_text = "\n".join([f"  • {r}" for r in self.failed_routes_list[:10]])  # Max 10 tane göster
            telegram_message += f"\n\n❌ <b>Başarısız Rotalar:</b>\n{failed_routes_text}"
            if len(self.failed_routes_list) > 10:
                telegram_message += f"\n  ... ve {len(self.failed_routes_list) - 10} rota daha"
        
        # Block rate uyarısı ekle
        if self.ban_monitor.get_block_rate() > 5:
            telegram_message += f"\n\n🚨 <b>UYARI:</b> Block rate yüksek! ({self.ban_monitor.get_block_rate():.1f}%)"
        
        send_telegram_message(telegram_message)
        
        return self.scraped_data


if __name__ == '__main__':
    # Database URL check
    if not os.getenv('DATABASE_URL', ''):
        logger.error("❌ DATABASE_URL environment variable not set!")
        logger.info("Usage: export DATABASE_URL='postgresql://user:pass@host:5432/dbname'")
        exit(1)
    
    # Scraper çalıştır
    scraper = ObiletScraper(
        max_workers=10,
        max_retries=20,
        batch_size=500
    )
    
    # Bugün için scrape et
    # cleanup_old_data=True → 30 günden eski verileri sil
    scraped_data = scraper.run(cleanup_old_data=True)
    
    logger.info("\n🎉 All operations completed successfully!")
