from sqlalchemy import Column, Integer, String, Float, Boolean, DateTime, ForeignKey
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship

Base = declarative_base()

class Property(Base):
    __tablename__ = "merged_property"

    ListingKey = Column(String, primary_key=True)
    ListingId = Column(String)
    ParcelNumber = Column(String)
    BedroomsTotal = Column(Integer)
    BathroomsTotalInteger = Column(Integer)
    Cooling = Column(String)
    Heating = Column(String)
    LotSizeAcres = Column(Float)
    PropertyType = Column(String)
    YearBuilt = Column(Integer)
    ZoningDescription = Column(String)
    TaxLegalDescription = Column(String)
    AssociationFee = Column(Float)
    InternetAddressDisplayYN = Column(Boolean)
    BathroomsFull = Column(Integer)
    BathroomsHalf = Column(Integer)
    PublicRemarks = Column(String)
    SubdivisionName = Column(String)
    TaxAnnualAmount = Column(Float)
    StandardStatus = Column(String)
    ListingContractDate = Column(DateTime)
    PostalCode = Column(String)
    Latitude = Column(Float)
    Longitude = Column(Float)
    City = Column(String)
    StateOrProvince = Column(String)
    CountyOrParish = Column(String)
    StreetName = Column(String)
    StreetNumber = Column(String)
    StreetSuffix = Column(String)
    UnparsedAddress = Column(String)
    LivingArea = Column(Float)
    ParkingFeatures = Column(String)
    PropertySubType = Column(String)
    Sewer = Column(String)
    WaterSource = Column(String)
    WaterfrontYN = Column(Boolean)
    ConstructionMaterials = Column(String)
    Utilities = Column(String)
    VirtualTourURLUnbranded = Column(String)
    OriginatingSystemName = Column(String)

    open_houses = relationship("OpenHouse", back_populates="property")

class OpenHouse(Base):
    __tablename__ = "merged_openhouse"

    OpenHouseKey = Column(String, primary_key=True)
    ListingKey = Column(String, ForeignKey("merged_property.ListingKey"))
    OpenHouseDate = Column(DateTime)
    OpenHouseStartTime = Column(DateTime)
    OpenHouseEndTime = Column(DateTime)
    OpenHouseRemarks = Column(String)
    OpenHouseStatus = Column(String)
    OpenHouseType = Column(String)

    property = relationship("Property", back_populates="open_houses")