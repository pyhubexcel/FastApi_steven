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

BATCH_SIZE = 10  # Fixed batch size


class Coordinate(BaseModel):
    latitude: float
    longitude: float

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
    UnparsedAddress: Optional[str] = None  
    LivingArea: Optional[float] = None 
    ParkingFeatures: Optional[str] = None 
    PropertySubType: Optional[str] = None  
    Sewer: Optional[str] = None  
    WaterSource: Optional[str] = None  
    WaterfrontYN: Optional[bool] = None  
    # constructionmaterials: Optional[str] = None  
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
    
    # New fields for bounding box geospatial filter
    # # Updated fields for bounding box geospatial filter
    northeast: Optional[Coordinate] = None  
    northwest: Optional[Coordinate] = None
    southeast: Optional[Coordinate] = None
    southwest: Optional[Coordinate] = None

     


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
    # constructionmaterials: Optional[str] = None  
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
                "image_url": item.get("MediaURL"),  
                **{k: v for k, v in item.items() if k in MediaResponse.__annotations__ and k != "MediaURL"}
            }
            for item in media_list
        ]
    except json.JSONDecodeError:
        return []





import redis
import json
from fastapi import HTTPException
from sqlalchemy import text

# Initialize Redis
r = redis.Redis(host='localhost', port=6379, db=0)


@app.post("/search", response_model=PaginatedResponse)
def search_properties(
    search: PropertySearch, 
    db: Session = Depends(get_db),
    page: int = Query(1, ge=1)
):
    # print("Received Payload:", search.dict())
    # Generate a unique key for the query based on search parameters and pagination
    query_key = f"search:{json.dumps(search.dict(exclude_unset=True))}:{page}"

    # Check if cached result exists
    cached_result = r.get(query_key)
    if cached_result:
        return json.loads(cached_result)

    # Base query
    query = 'SELECT * FROM merged_property WHERE 1=1'
    params = {}

    # Apply other filters from the search payload (excluding geospatial fields)
    for field, value in search.dict(exclude_unset=True).items():
        if value is not None and field not in ['northeast', 'northwest', 'southeast', 'southwest']:
            query += f' AND "{field}" = :{field}'
            params[field] = value
    
    # Geospatial filter (bounding box for latitude and longitude)
    if search.northeast and search.southwest:
        # Extract latitude and longitude from coordinates
        northeast_latitude = search.northeast.latitude
        northeast_longitude = search.northeast.longitude
        southwest_latitude = search.southwest.latitude
        southwest_longitude = search.southwest.longitude
        
        query += ' AND CAST("Latitude" AS FLOAT) BETWEEN :southwest_latitude AND :northeast_latitude'
        query += ' AND CAST("Longitude" AS FLOAT) BETWEEN :southwest_longitude AND :northeast_longitude'
        
        # Update parameters for latitudes and longitudes
        params['northeast_latitude'] = northeast_latitude
        params['southwest_latitude'] = southwest_latitude
        params['northeast_longitude'] = northeast_longitude
        params['southwest_longitude'] = southwest_longitude


    # Sort by ListingId
    query += ' ORDER BY "ListingId"'

    # Pagination
    offset = (page - 1) * BATCH_SIZE
    query += ' LIMIT :size OFFSET :offset'
    params['size'] = BATCH_SIZE
    params['offset'] = offset
    # print(query)
    # Execute the main query
    result = db.execute(text(query), params)
    properties = result.fetchall()

    # Get total count for pagination
    count_query = 'SELECT COUNT(*) FROM merged_property WHERE 1=1'
    for field, value in search.dict(exclude_unset=True).items():
        if value is not None and field not in ['northeast', 'northwest', 'southeast', 'southwest']:
            count_query += f' AND "{field}" = :{field}'
    
    # Geospatial filter for count query
    if search.northeast and search.southwest:
        count_query += ' AND CAST("Latitude" AS FLOAT) BETWEEN :southwest_latitude AND :northeast_latitude'
        count_query += ' AND  CAST("Longitude" AS FLOAT) BETWEEN :southwest_longitude AND :northeast_longitude'

    total_count = db.execute(text(count_query), params).scalar()

    # Raise 404 if no properties found
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
        prop_dict['materials'] = prop_dict.pop('ConstructionMaterials', None)
        prop_dict['type'] = prop_dict.pop('PropertyType', None)
        prop_dict['address'] = prop_dict.pop('UnparsedAddress', None)
        prop_dict['price'] = prop_dict.pop('ListPrice', None)


        # Add new fields that may be missing 
        prop_dict['appliances'] = None  # Default to None if missing
        prop_dict['features'] = None
        prop_dict['attached_garage'] = None
        prop_dict['size'] = None
        prop_dict['built_in'] = None
        # prop_dict['price'] = None
        prop_dict['description'] = None

        # Fetch open houses and add to property
        open_houses = db.query(OpenHouse).filter(OpenHouse.ListingKey == prop_dict.get('ListingKey')).all()
        prop_dict['open_house_times'] = [OpenHouseResponse(**oh.__dict__) for oh in open_houses]

        # Add property to results
        results.append(PropertyResponse(**prop_dict))

    response = PaginatedResponse(
        items=results,
        total=total_count,
        page=page,
        size=BATCH_SIZE
    )

    # # Cache the result in Redis for future queries (set to expire after 10 minutes)
    r.set(query_key, json.dumps(response.dict()), ex=600)

    return response



# # ###cluster property data ############

import redis
import json
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from database import get_db
from typing import List, Dict
from pydantic import BaseModel
from sqlalchemy import text, cast, Float

# Initialize Redis
r = redis.Redis(host='localhost', port=6379, db=0)

class Coordinate(BaseModel):
    latitude: float
    longitude: float

class MapFocus(BaseModel):
    northeast: Coordinate
    northwest: Coordinate
    southeast: Coordinate
    southwest: Coordinate

class PropertyDetails(BaseModel):
    Latitude: float
    Longitude: float
    ListingId: str 
    property_id: str 

class ClusteringResponse(BaseModel):
    clusters: List[Dict]  

@app.post("/cluster_properties_data", response_model=ClusteringResponse)
def cluster_properties(
    map_focus: MapFocus,
    db: Session = Depends(get_db),
):
    # Generate a unique cache key based on map focus coordinates
    query_key = f"cluster:{map_focus.northeast.latitude},{map_focus.northeast.longitude}:" \
                f"{map_focus.southwest.latitude},{map_focus.southwest.longitude}"

    # # Check if cached result exists in Redis
    cached_result = r.get(query_key)
    if cached_result:
        return ClusteringResponse(**json.loads(cached_result))

    # Extract map focus coordinates
    north_east_latitude = map_focus.northeast.latitude
    north_east_longitude = map_focus.northeast.longitude
    south_west_latitude = map_focus.southwest.latitude
    south_west_longitude = map_focus.southwest.longitude

    # Query to cast Latitude and Longitude as Float
    properties = db.query(
        cast(Property.Latitude, Float),
        cast(Property.Longitude, Float),
        Property.ListingId,
        Property.ListPrice
    ).filter(
        cast(Property.Latitude, Float).between(south_west_latitude, north_east_latitude),
        cast(Property.Longitude, Float).between(south_west_longitude, north_east_longitude),
        Property.StandardStatus.in_(["Active", "Pending", "Coming Soon"])
    ).all()

    if not properties:
        raise HTTPException(status_code=404, detail="No properties found in this area")

    # Process the query result
    response_clusters = []

    # Here, we only add property details (without 'location' and 'count')
    for prop in properties:
        response_clusters.append({
            "latitude": float(prop[0]),
            "longitude": float(prop[1]),
            "listingId": str(prop[2]),
            "price": str(prop[3])
        })

    # Prepare the final response
    response = ClusteringResponse(clusters=response_clusters)

    # Cache the result in Redis with an expiration time of 10 minutes (600 seconds)
    r.set(query_key, json.dumps(response.dict()), ex=60000)

    return response



#####midpointAPI########


import redis
import json
from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session
from database import get_db
from typing import List, Dict
from pydantic import BaseModel
from sqlalchemy import text

# Initialize Redis
r = redis.Redis(host='localhost', port=6379, db=0)

class Coordinate(BaseModel):
    latitude: float
    longitude: float

class MapFocus(BaseModel):
    northeast: Coordinate
    northwest: Coordinate
    southeast: Coordinate
    southwest: Coordinate

class MidpointCluster(BaseModel):
    latitude: float  
    longitude: float  
    count: int

class MidpointsResponse(BaseModel):
    midpoints: List[MidpointCluster]

@app.post("/cluster_property_mid_point", response_model=MidpointsResponse)
def cluster_properties_mid(
    map_focus: MapFocus,
    db: Session = Depends(get_db),
):
    # Generate a unique cache key based on map focus coordinates
    query_key = f"midpoints:{map_focus.northeast.latitude},{map_focus.northeast.longitude}:" \
                f"{map_focus.southwest.latitude},{map_focus.southwest.longitude}"

    # Check if cached result exists in Redis
    cached_result = r.get(query_key)
    if cached_result:
        return MidpointsResponse(**json.loads(cached_result))

    north_east_latitude = map_focus.northeast.latitude
    north_east_longitude = map_focus.northeast.longitude
    south_west_latitude = map_focus.southwest.latitude
    south_west_longitude = map_focus.southwest.longitude

    # Define the statuses you want to filter
    desired_statuses = ["Active", "Pending", "Coming Soon"]

    query = text(""" 
        SELECT "Latitude", "Longitude" FROM merged_property 
        WHERE CAST("Latitude" AS FLOAT) BETWEEN :south_west_latitude AND :north_east_latitude 
        AND CAST("Longitude" AS FLOAT) BETWEEN :south_west_longitude AND :north_east_longitude
        AND "StandardStatus" IN :desired_statuses
    """)
    params = {
        "south_west_latitude": south_west_latitude,
        "north_east_latitude": north_east_latitude,
        "south_west_longitude": south_west_longitude,
        "north_east_longitude": north_east_longitude,
        "desired_statuses": tuple(desired_statuses),
    }
    result = db.execute(query, params)
    properties = result.fetchall()

    if not properties:
        raise HTTPException(status_code=404, detail="No properties found in this area with the specified statuses")

    # Clustering logic: grid-based approach
    cluster_size = 0.01  # Size of each cluster grid cell (degrees of latitude/longitude)
    clusters = {}

    for prop in properties:
        lat = float(prop.Latitude)
        lng = float(prop.Longitude)
        
        # Compute the grid cell for this property
        grid_lat = round(lat / cluster_size)
        grid_lng = round(lng / cluster_size)
        grid_key = (grid_lat, grid_lng)
        
        # Add the property to the corresponding cluster
        if grid_key not in clusters:
            clusters[grid_key] = {
                "sum_lat": 0,
                "sum_lng": 0,
                "count": 0,
            }
        
        clusters[grid_key]["sum_lat"] += lat
        clusters[grid_key]["sum_lng"] += lng
        clusters[grid_key]["count"] += 1

    # Prepare the response
    midpoints = []
    total_clusters = len(clusters)

    if total_clusters > 60:
        # Reduce clusters by merging those with few properties into larger clusters
        cluster_threshold = 60  # Maximum number of clusters we want to return
        sorted_clusters = sorted(clusters.values(), key=lambda x: -x["count"])
        
        for cluster_data in sorted_clusters[:cluster_threshold]:
            avg_latitude = cluster_data["sum_lat"] / cluster_data["count"]
            avg_longitude = cluster_data["sum_lng"] / cluster_data["count"]
            midpoints.append(MidpointCluster(
                latitude=avg_latitude,
                longitude=avg_longitude,
                count=cluster_data["count"]
            ))

        # Merge remaining clusters into the top 60 clusters by increasing their count
        remaining_clusters = sorted_clusters[cluster_threshold:]
        for cluster_data in remaining_clusters:
            closest_cluster = min(midpoints, key=lambda c: (c.latitude - (cluster_data["sum_lat"] / cluster_data["count"]))**2 + (c.longitude - (cluster_data["sum_lng"] / cluster_data["count"]))**2)
            closest_cluster.count += cluster_data["count"]

    else:
        # If clusters are 60 or less, just add them all
        for cluster_data in clusters.values():
            avg_latitude = cluster_data["sum_lat"] / cluster_data["count"]
            avg_longitude = cluster_data["sum_lng"] / cluster_data["count"]
            midpoints.append(MidpointCluster(
                latitude=avg_latitude,
                longitude=avg_longitude,
                count=cluster_data["count"]
            ))

    response = MidpointsResponse(midpoints=midpoints)

    # Cache the result in Redis with an expiration time of 10 minutes (600 seconds)
    r.set(query_key, json.dumps(response.dict()), ex=600)

    return response





#cluster table logic##########




from sqlalchemy import Column, Integer, Float, String, Numeric
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()

class PropertyCluster(Base):
    __tablename__ = 'property_clusters'
    
    id = Column(Integer, primary_key=True, index=True)
    Latitude = Column(Float)
    Longitude = Column(Float)
    ListingId = Column(String)
    ListPrice = Column(Numeric)
    StandardStatus = Column(String)
    cluster_value = Column(Integer)

from database import engine
from models import Base

Base.metadata.create_all(bind=engine) 

@app.post("/create-cluster-table/")
def create_cluster_table(db: Session = Depends(get_db)):
    # Create the table if it does not exist
    PropertyCluster.__table__.create(bind=engine, checkfirst=True)
    return {"message": "property_clusters table created successfully"}


@app.post("/run-clustering/")
def run_clustering(db: Session = Depends(get_db)):
    try:
        # Query to display the schema of property_clusters
        schema_clusters_query = text("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'property_clusters'
        """)
        
        schema_clusters_result = db.execute(schema_clusters_query).fetchall()
        schema_clusters_info = [{"column_name": row[0], "data_type": row[1]} for row in schema_clusters_result]

        # Query to display the schema of merged_property
        schema_merged_query = text("""
        SELECT column_name, data_type 
        FROM information_schema.columns 
        WHERE table_name = 'merged_property'
        """)
        
        schema_merged_result = db.execute(schema_merged_query).fetchall()
        schema_merged_info = [{"column_name": row[0], "data_type": row[1]} for row in schema_merged_result]
        
        print("property_clusters schema:", schema_clusters_info)
        print("merged_property schema:", schema_merged_info)

        # Clustering SQL with corrected column names
        clustering_sql = text("""
        INSERT INTO property_clusters (latitude, longitude, listingid, listprice, standardstatus, cluster_value)
        SELECT latitude, longitude, listingid, listprice, standardstatus, FLOOR(RANDOM() * 10 + 1)
        FROM merged_property
        WHERE standardstatus IN ('Active', 'Pending', 'Coming Soon')
        """)
        db.execute(clustering_sql)
        db.commit()

        return {
            "message": "Clustering data inserted successfully into property_clusters",
            "schema_info_clusters": schema_clusters_info,
            "schema_info_merged": schema_merged_info
        }

    except Exception as e:
        db.rollback()
        raise HTTPException(status_code=500, detail=f"An error occurred: {str(e)}")






# Pydantic model for API response
class PropertyClusterResponse(BaseModel):
    id: int
    Latitude: Optional[float]
    Longitude: Optional[float]
    ListingId: Optional[str]
    ListPrice: Optional[float]
    StandardStatus: Optional[str]
    cluster_value: Optional[int]
# API to fetch all clustered data
@app.get("/clusters/", response_model=List[PropertyClusterResponse])
def get_clusters(db: Session = Depends(get_db)):
    clusters = db.query(PropertyCluster).all()
    if not clusters:
        raise HTTPException(status_code=404, detail="No clusters found")
    return clusters

if __name__ == "__main__":
    uvicorn.run(app, host="192.168.36.100", port=9999)


