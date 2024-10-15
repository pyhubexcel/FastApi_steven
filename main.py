from fastapi import FastAPI, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from fastapi.middleware.cors import CORSMiddleware
from typing import List, Optional, Dict, Any
from pydantic import BaseModel
from database import get_db
from models import Property, OpenHouse
from sqlalchemy import text
import uvicorn
import json


app = FastAPI()
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://192.168.12.121"], 
    allow_credentials=True,
    allow_methods=["*"],  
    allow_headers=["*"],  
)

BATCH_SIZE = 20  # Fixed batch size

class PropertySearch(BaseModel):
    ListingId: Optional[str] = None  
    ParcelNumber: Optional[str] = None  
    beds: Optional[int] = None  
    baths: Optional[int] = None 
    full_bathrooms: Optional[int] = None  
    half_bathrooms: Optional[int] = None  
    Cooling: Optional[str] = None  
    Heating: Optional[str] = None  
    LotSizeAcres: Optional[float] = None  
    type: Optional[str] = None  
    YearBuilt: Optional[int] = None  
    ZoningDescription: Optional[str] = None  
    TaxLegalDescription: Optional[str] = None  
    AssociationFee: Optional[float] = None  
    InternetAddressDisplayYN: Optional[bool] = None  
    SubdivisionName: Optional[str] = None  
    TaxAnnualAmount: Optional[float] = None  
    StandardStatus: Optional[str] = None  
    ListingContractDate: Optional[str] = None  
    PostalCode: Optional[str] = None  
    latitude: Optional[float] = None  
    longitude: Optional[float] = None  
    City: Optional[str] = None  
    StateOrProvince: Optional[str] = None  
    CountyOrParish: Optional[str] = None  
    StreetName: Optional[str] = None  
    StreetNumber: Optional[str] = None  
    StreetSuffix: Optional[str] = None  
    address: Optional[str] = None  
    LivingArea: Optional[float] = None 
    ParkingFeatures: Optional[str] = None 
    PropertySubType: Optional[str] = None  
    Sewer: Optional[str] = None  
    WaterSource: Optional[str] = None  
    WaterfrontYN: Optional[bool] = None  
    construction: Optional[str] = None  
    materials: Optional[str] = None  
    Utilities: Optional[str] = None  
    VirtualTourURLUnbranded: Optional[str] = None  
    OriginatingSystemName: Optional[str] = None  
    appliances: Optional[str] = None  
    features: Optional[str] = None  
    attached_garage: Optional[bool] = None  
    size: Optional[str] = None  
    built_in: Optional[int] = None  
    price: Optional[float] = None  
    description: Optional[str] = None 
     


class OpenHouseResponse(BaseModel):
    OpenHouseDate: Optional[str] = None
    OpenHouseStartTime: Optional[str] = None
    OpenHouseEndTime: Optional[str] = None
    OpenHouseRemarks: Optional[str] = None
    OpenHouseStatus: Optional[str] = None
    OpenHouseType: Optional[str] = None


class MediaResponse(BaseModel):
    image_url: Optional[str] = None  
    MediaType: Optional[str] = None  
    Order: Optional[int] = None  
    ResourceRecordKey: Optional[str] = None  
    label: Optional[str] = None 
    ImageWidth: Optional[int] = None  
    ImageHeight: Optional[int] = None  
    ImageSizeDescription: Optional[str] = None  
    MediaModificationTimestamp: Optional[str] = None  
    MediaKey: Optional[str] = None  


class PropertyResponse(BaseModel):
    ListingId: Optional[str] = None  
    ParcelNumber: Optional[str] = None  
    beds: Optional[int] = None  
    baths: Optional[int] = None 
    full_bathrooms: Optional[int] = None  
    half_bathrooms: Optional[int] = None  
    Cooling: Optional[str] = None  
    Heating: Optional[str] = None  
    LotSizeAcres: Optional[float] = None  
    type: Optional[str] = None  
    YearBuilt: Optional[int] = None  
    ZoningDescription: Optional[str] = None  
    TaxLegalDescription: Optional[str] = None  
    AssociationFee: Optional[float] = None  
    InternetAddressDisplayYN: Optional[bool] = None  
    SubdivisionName: Optional[str] = None  
    TaxAnnualAmount: Optional[float] = None  
    StandardStatus: Optional[str] = None  
    ListingContractDate: Optional[str] = None  
    PostalCode: Optional[str] = None  
    latitude: Optional[float] = None  
    longitude: Optional[float] = None  
    City: Optional[str] = None  
    StateOrProvince: Optional[str] = None  
    CountyOrParish: Optional[str] = None  
    StreetName: Optional[str] = None  
    StreetNumber: Optional[str] = None  
    StreetSuffix: Optional[str] = None  
    address: Optional[str] = None  
    LivingArea: Optional[float] = None 
    ParkingFeatures: Optional[str] = None 
    PropertySubType: Optional[str] = None  
    Sewer: Optional[str] = None  
    WaterSource: Optional[str] = None  
    WaterfrontYN: Optional[bool] = None  
    construction: Optional[str] = None  
    materials: Optional[str] = None  
    Utilities: Optional[str] = None  
    VirtualTourURLUnbranded: Optional[str] = None  
    OriginatingSystemName: Optional[str] = None  
    appliances: Optional[str] = None  
    features: Optional[str] = None  
    attached_garage: Optional[bool] = None  
    size: Optional[str] = None  
    built_in: Optional[int] = None  
    price: Optional[float] = None  
    description: Optional[str] = None  
    images: Optional[List[MediaResponse]] = None  
    open_house_times: Optional[List[OpenHouseResponse]] = None  



class ListingResponse(BaseModel):
    property: PropertyResponse

class PaginatedResponse(BaseModel):
    items: List
    total: int
    page: int
    size: int

def parse_media(media_data: str) -> List[Dict[str, Any]]:
    try:
        media_list = json.loads(media_data)

        return [
            {
                "image_url": item.get("MediaURL"),  # Map MediaURL to image_url
                **{k: v for k, v in item.items() if k in MediaResponse.__annotations__ and k != "MediaURL"}
            }
            for item in media_list
        ]
    except json.JSONDecodeError:
        return []

@app.post("/search", response_model=PaginatedResponse)
def search_properties(
    search: PropertySearch, 
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1)
):
    query = 'SELECT * FROM merged_property WHERE 1=1  '
    params = {}

    for field, value in search.dict(exclude_unset=True).items():
        if value is not None:
            query += f' AND "{field}" = :{field}'
            params[field] = value

    # Add pagination
    offset = (page - 1) * BATCH_SIZE
    query += f' LIMIT :size OFFSET :offset'
    params['size'] = BATCH_SIZE
    params['offset'] = offset

    # Execute the main query
    result = db.execute(text(query), params)
    properties = result.fetchall()

    # Get total count
    count_query = f'SELECT COUNT(*) FROM merged_property WHERE 1=1'
    for field, value in search.dict(exclude_unset=True).items():
        if value is not None:
            count_query += f' AND "{field}" = :{field}'
    total_count = db.execute(text(count_query), params).scalar()

    if not properties:
        raise HTTPException(status_code=404, detail="No properties found")

    results = []

    for prop in properties:
      
        prop_dict = {column: value for column, value in prop._mapping.items()}
        
        # Parse Media (images) and map fields
        if prop_dict.get('Media'):
            prop_dict['images'] = [MediaResponse(**item) for item in parse_media(prop_dict['Media'])]
        else:
            prop_dict['images'] = []

        # Update field mappings
        prop_dict['full_bathrooms'] = prop_dict.pop('BathroomsFull', None)
        prop_dict['half_bathrooms'] = prop_dict.pop('BathroomsHalf', None)
        prop_dict['beds'] = prop_dict.pop('BedroomsTotal', None)
        prop_dict['baths'] = prop_dict.pop('BathroomsTotalInteger', None)
        prop_dict['latitude'] = prop_dict.pop('Latitude')
        prop_dict['longitude'] = prop_dict.pop('Longitude')
        prop_dict['construction'] = prop_dict.pop('ConstructionMaterials', None)
        prop_dict['type'] = prop_dict.pop('PropertyType', None)
        prop_dict['address'] = prop_dict.pop('UnparsedAddress', None)

        # Add new fields that may be missing 
        prop_dict['appliances'] = None  # Default to None if missing
        prop_dict['features'] = None
        prop_dict['attached_garage'] = None
        prop_dict['size'] = None
        prop_dict['built_in'] = None
        prop_dict['price'] = None
        prop_dict['description'] = None

        
         # Fetch open houses and add to property
        open_houses = db.query(OpenHouse).filter(OpenHouse.ListingKey == prop_dict.get('ListingKey')).all()
        prop_dict['open_house_times'] = [OpenHouseResponse(**oh.__dict__) for oh in open_houses]

        results.append( PropertyResponse(**prop_dict))

    return PaginatedResponse(
        items=results,
        total=total_count,
        page=page,
        size=BATCH_SIZE
    )

if __name__ == "__main__":
    uvicorn.run(app, host="192.168.36.100", port=9999)





    
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from database import get_db
from typing import List, Dict
from pydantic import BaseModel
from sqlalchemy import text

# Define a model for a corner coordinate
class CornerCoordinate(BaseModel):
    lat: float
    lng: float

# Update the MapFocus model to accept a list of corner coordinates
class MapFocus(BaseModel):
    corners: List[CornerCoordinate]

class ClusteringResponse(BaseModel):
    clusters: List[Dict[str, float]]  # Adjusted to return clusters or individual properties
    total_count: int

@app.post("/cluster_properties", response_model=ClusteringResponse)
def cluster_properties(
    map_focus: MapFocus,
    db: Session = Depends(get_db),
):
    # Extract coordinates from the corners
    north_east_latitude = max(corner.lat for corner in map_focus.corners)
    north_east_longitude = max(corner.lng for corner in map_focus.corners)
    south_west_latitude = min(corner.lat for corner in map_focus.corners)
    south_west_longitude = min(corner.lng for corner in map_focus.corners)

    # Query to get properties within the bounding box
    query = text("""
        SELECT * FROM merged_property
        WHERE latitude BETWEEN :south_west_latitude AND :north_east_latitude
        AND longitude BETWEEN :south_west_longitude AND :north_east_longitude
    """)
    params = {
        "south_west_latitude": south_west_latitude,
        "north_east_latitude": north_east_latitude,
        "south_west_longitude": south_west_longitude,
        "north_east_longitude": north_east_longitude,
    }

    result = db.execute(query, params)
    properties = result.fetchall()

    if not properties:
        raise HTTPException(status_code=404, detail="No properties found in this area")

    # Implement clustering logic
    clusters = []  # This should contain your clustering logic
    cluster_map = {}
    
    for prop in properties:
        key = (round(prop.latitude, 1), round(prop.longitude, 1))  # Cluster by rounded lat/lng
        if key not in cluster_map:
            cluster_map[key] = {
                "latitude": prop.latitude,
                "longitude": prop.longitude,
                "count": 0
            }
        cluster_map[key]["count"] += 1

    clusters = [{"location": k, "count": v["count"]} for k, v in cluster_map.items()]

    return ClusteringResponse(clusters=clusters, total_count=len(properties))




if __name__ == "__main__":
    uvicorn.run(app, host="192.168.36.100", port=9999)


