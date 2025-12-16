"""
SeferTakip - Database Models (Standalone)
Pure SQLAlchemy - Flask olmadan kullanılabilir
Consumer, Scraper, Standalone scriptler için

NOT: Bu dosya models.py'nin kopyasıdır.
Her migration'dan sonra models.py'den sync edilmelidir.
"""

from sqlalchemy import Column, Integer, String, Boolean, DateTime, Numeric, Text, Date, ForeignKey, JSON, CheckConstraint, UniqueConstraint
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
import uuid

# ============================================
# BASE CLASS
# ============================================
Base = declarative_base()


# ============================================
# MODELS
# ============================================

class User(Base):
    """
    Kullanıcılar (Admin + Firmalar)
    """
    __tablename__ = 'users'
    
    id = Column(Integer, primary_key=True)
    
    # Basic Info
    company_name = Column(String(200), nullable=False)
    email = Column(String(200), unique=True, nullable=False, index=True)
    password_hash = Column(String(255), nullable=False)
    phone = Column(String(20))
    
    # Role & Permissions
    role = Column(String(20), nullable=False, default='company', index=True)  # 'admin' or 'company'
    subscription_plan = Column(String(50), default='basic')  # 'basic', 'pro', 'enterprise'
    max_tracked_routes = Column(Integer, default=10)
    max_alerts_per_month = Column(Integer, default=100)
    
    # Status & Timestamps
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    last_login = Column(DateTime)
    
    # Self-referencing relationship (admin creates companies)
    created_by_id = Column(Integer, ForeignKey('users.id'))
    created_by = relationship('User', remote_side=[id], backref='created_users')
    
    # Relationships
    company_routes = relationship('CompanyRoute', back_populates='user', cascade='all, delete-orphan')
    qr_codes = relationship('QRCode', back_populates='user', cascade='all, delete-orphan')
    feedbacks = relationship('CustomerFeedback', back_populates='user', cascade='all, delete-orphan')
    price_alerts = relationship('PriceAlert', back_populates='user', cascade='all, delete-orphan')
    notifications = relationship('Notification', back_populates='user', cascade='all, delete-orphan')
    
    def set_password(self, password):
        """Şifreyi hash'le"""
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password):
        """Şifreyi kontrol et"""
        return check_password_hash(self.password_hash, password)
    
    def is_admin(self):
        """Admin mi?"""
        return self.role == 'admin'
    
    def is_company(self):
        """Firma mı?"""
        return self.role == 'company'
    
    def can_add_route(self):
        """Yeni rota ekleyebilir mi?"""
        current_routes = len([cr for cr in self.company_routes if cr.is_active])
        return current_routes < self.max_tracked_routes
    
    def __repr__(self):
        return f'<User {self.company_name} ({self.role})>'


class Route(Base):
    """
    Güzergahlar (Shared - Tüm firmalar kullanabilir)
    """
    __tablename__ = 'routes'
    
    id = Column(Integer, primary_key=True)
    
    # Origin
    origin_city_name = Column(String(100), nullable=False)
    origin_city_code = Column(String(10))
    origin_obilet_id = Column(Integer, nullable=False, index=True)
    
    # Destination
    destination_city_name = Column(String(100), nullable=False)
    destination_city_code = Column(String(10))
    destination_obilet_id = Column(Integer, nullable=False, index=True)
    
    # Route Info
    route_name = Column(String(200))
    distance_km = Column(Integer)
    estimated_duration = Column(Integer)  # minutes
    is_active = Column(Boolean, default=True, nullable=False, index=True)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Unique constraint
    __table_args__ = (
        UniqueConstraint('origin_obilet_id', 'destination_obilet_id', name='unique_route'),
    )
    
    # Relationships
    company_routes = relationship('CompanyRoute', back_populates='route', cascade='all, delete-orphan')
    journeys = relationship('Journey', back_populates='route', cascade='all, delete-orphan')
    price_history = relationship('PriceHistory', back_populates='route', cascade='all, delete-orphan')
    price_alerts = relationship('PriceAlert', back_populates='route')
    
    def get_obilet_url(self, date_str):
        """Obilet API URL'i oluştur"""
        return f"/json/journeys/{self.origin_obilet_id}-{self.destination_obilet_id}/{date_str}"
    
    def __repr__(self):
        return f'<Route {self.route_name or f"{self.origin_city_name} - {self.destination_city_name}"}>'


class CompanyRoute(Base):
    """
    Firma-Rota İlişkisi (Junction Table - Many to Many)
    Her firmanın hangi rotaları takip ettiği
    """
    __tablename__ = 'company_routes'
    
    id = Column(Integer, primary_key=True)
    
    # Foreign Keys
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    route_id = Column(Integer, ForeignKey('routes.id', ondelete='CASCADE'), nullable=False)
    
    # Alert Settings (Per Company)
    alert_on_price_change = Column(Boolean, default=True)
    alert_on_low_availability = Column(Boolean, default=True)
    alert_threshold_percentage = Column(Numeric(5, 2), default=10.0)  # %10
    
    # Notification Preferences
    notify_email = Column(Boolean, default=True)
    notify_whatsapp = Column(Boolean, default=False)
    
    # Status
    is_active = Column(Boolean, default=True, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Unique constraint
    __table_args__ = (
        UniqueConstraint('user_id', 'route_id', name='unique_user_route'),
    )
    
    # Relationships
    user = relationship('User', back_populates='company_routes')
    route = relationship('Route', back_populates='company_routes')
    
    def __repr__(self):
        return f'<CompanyRoute User:{self.user_id} Route:{self.route_id}>'


class Journey(Base):
    """
    Seferler (Scraper'dan gelen veriler)
    """
    __tablename__ = 'journeys'
    
    id = Column(Integer, primary_key=True)
    route_id = Column(Integer, ForeignKey('routes.id', ondelete='CASCADE'), nullable=False)
    
    # Company Info
    company_name = Column(String(200), nullable=False, index=True)
    obilet_partner_id = Column(Integer)
    
    # Journey Details
    departure_time = Column(DateTime, nullable=False, index=True)
    arrival_time = Column(DateTime)
    duration = Column(Integer)  # minutes
    
    # Pricing
    original_price = Column(Numeric(10, 2))
    internet_price = Column(Numeric(10, 2), nullable=False)
    currency = Column(String(3), default='TRY')
    
    # Availability
    total_seats = Column(Integer)
    available_seats = Column(Integer)
    occupancy_rate = Column(Numeric(5, 2))  # Percentage
    
    # Bus Info
    bus_type = Column(String(50))
    bus_plate = Column(String(20))
    has_wifi = Column(Boolean, default=False)
    has_usb = Column(Boolean, default=False)
    has_tv = Column(Boolean, default=False)
    has_socket = Column(Boolean, default=False)
    
    # Metadata
    obilet_journey_id = Column(String(100), unique=True)
    scraped_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    is_active = Column(Boolean, default=True)
    
    # Relationships
    route = relationship('Route', back_populates='journeys')
    
    def __repr__(self):
        return f'<Journey {self.company_name} {self.departure_time}>'


class PriceHistory(Base):
    """
    Fiyat Geçmişi (Time Series Data)
    """
    __tablename__ = 'price_history'
    
    id = Column(Integer, primary_key=True)
    route_id = Column(Integer, ForeignKey('routes.id', ondelete='CASCADE'), nullable=False)
    
    # Company
    company_name = Column(String(200), nullable=False)
    obilet_partner_id = Column(Integer)
    
    # Price Data
    price = Column(Numeric(10, 2), nullable=False)
    currency = Column(String(3), default='TRY')
    
    # Context
    departure_date = Column(Date, nullable=False, index=True)
    days_before_departure = Column(Integer)
    
    # Availability
    available_seats = Column(Integer)
    total_seats = Column(Integer)
    occupancy_rate = Column(Numeric(5, 2))
    
    # Metadata
    recorded_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    # Relationships
    route = relationship('Route', back_populates='price_history')
    
    def __repr__(self):
        return f'<PriceHistory {self.company_name} {self.price} {self.departure_date}>'


class QRCode(Base):
    """
    QR Kodlar (Feedback için)
    """
    __tablename__ = 'qr_codes'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    
    # Vehicle Info
    plate_number = Column(String(20), nullable=False)
    bus_type = Column(String(50))
    
    # QR Data
    qr_code_uuid = Column(String(36), unique=True, nullable=False, default=lambda: str(uuid.uuid4()), index=True)
    qr_url = Column(Text)
    
    # Statistics
    scan_count = Column(Integer, default=0)
    feedback_count = Column(Integer, default=0)
    is_active = Column(Boolean, default=True, nullable=False)
    
    # Timestamps
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    
    # Unique constraint
    __table_args__ = (
        UniqueConstraint('user_id', 'plate_number', name='unique_user_plate'),
    )
    
    # Relationships
    user = relationship('User', back_populates='qr_codes')
    feedbacks = relationship('CustomerFeedback', back_populates='qr_code')
    
    def __repr__(self):
        return f'<QRCode {self.plate_number}>'


class CustomerFeedback(Base):
    """
    Müşteri Geri Bildirimleri
    """
    __tablename__ = 'customer_feedbacks'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    qr_code_id = Column(Integer, ForeignKey('qr_codes.id', ondelete='SET NULL'))
    
    # Vehicle
    plate_number = Column(String(20))
    
    # Customer Info (Optional)
    customer_name = Column(String(200))
    customer_email = Column(String(200))
    
    # Rating & Feedback
    rating = Column(Integer, nullable=False)  # 1-5
    quick_tags = Column(JSON)  # ["Konfor", "Temizlik", "Dakiklik"]
    comment = Column(Text)
    
    # Journey Details (Optional)
    journey_date = Column(Date)
    route_info = Column(String(200))
    
    # Status
    is_published = Column(Boolean, default=True)
    
    # Metadata
    ip_address = Column(String(50))
    user_agent = Column(Text)
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    # Check constraint
    __table_args__ = (
        CheckConstraint('rating >= 1 AND rating <= 5', name='valid_rating'),
    )
    
    # Relationships
    user = relationship('User', back_populates='feedbacks')
    qr_code = relationship('QRCode', back_populates='feedbacks')
    
    def __repr__(self):
        return f'<CustomerFeedback {self.rating}⭐ {self.plate_number}>'


class PriceAlert(Base):
    """
    Fiyat Uyarıları
    """
    __tablename__ = 'price_alerts'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    route_id = Column(Integer, ForeignKey('routes.id', ondelete='SET NULL'))
    
    # Alert Type
    alert_type = Column(String(50), nullable=False)  # 'price_drop', 'price_increase', 'low_availability'
    
    # Alert Details
    title = Column(String(200), nullable=False)
    message = Column(Text, nullable=False)
    
    # Price Info
    competitor_name = Column(String(200))
    old_price = Column(Numeric(10, 2))
    new_price = Column(Numeric(10, 2))
    price_change_percentage = Column(Numeric(5, 2))
    
    # Availability Info
    available_seats = Column(Integer)
    total_seats = Column(Integer)
    occupancy_rate = Column(Numeric(5, 2))
    
    # Context
    route_info = Column(String(200))
    departure_date = Column(Date)
    
    # Status
    is_read = Column(Boolean, default=False, nullable=False)
    is_sent = Column(Boolean, default=False)
    sent_at = Column(DateTime)
    priority = Column(String(20), default='medium')  # 'low', 'medium', 'high', 'urgent'
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    # Relationships
    user = relationship('User', back_populates='price_alerts')
    route = relationship('Route', back_populates='price_alerts')
    
    def __repr__(self):
        return f'<PriceAlert {self.alert_type} for User:{self.user_id}>'


class Notification(Base):
    """
    Genel Bildirimler
    """
    __tablename__ = 'notifications'
    
    id = Column(Integer, primary_key=True)
    user_id = Column(Integer, ForeignKey('users.id', ondelete='CASCADE'), nullable=False)
    
    # Notification Details
    title = Column(String(200), nullable=False)
    message = Column(Text, nullable=False)
    notification_type = Column(String(50), default='info')  # 'info', 'success', 'warning', 'error'
    
    # Action (Optional)
    action_url = Column(String(500))
    action_label = Column(String(100))
    icon = Column(String(50))
    
    # Status
    is_read = Column(Boolean, default=False, nullable=False)
    read_at = Column(DateTime)
    priority = Column(String(20), default='normal')  # 'low', 'normal', 'high'
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False, index=True)
    
    # Relationships
    user = relationship('User', back_populates='notifications')
    
    def __repr__(self):
        return f'<Notification {self.title} for User:{self.user_id}>'
