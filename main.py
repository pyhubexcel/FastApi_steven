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





# @app.post("/search2", response_model=PaginatedResponse)
# def search_properties1(
#     search: PropertySearch, 
#     db: Session = Depends(get_db),
#     page: int = Query(1, ge=1)
# ):
#     # Generate a unique key for the query based on search parameters and pagination
#     query_key = f"search:{json.dumps(search.dict(exclude_unset=True))}:{page}"

#     # Check if cached result exists
#     cached_result = r.get(query_key)
#     if cached_result:
#         return json.loads(cached_result)

#     # Base query with join between property_clusters and merged_property
#     query = '''
#     SELECT 
#         pc."Latitude", 
#         pc."Longitude", 
#         pc."ListingId", 
#         pc."ListPrice", 
#         pc."StandardStatus",
#         mp.*  -- Select all other fields from merged_property
#     FROM 
#         property_clusters pc
#     JOIN 
#         merged_property mp ON pc."ListingId" = mp."ListingId"
#     WHERE 
#         pc."StandardStatus" = :status
#     '''
#     params = {"status": "Active"}
    
#     # Apply other filters from the search payload (excluding geospatial fields)
#     for field, value in search.dict(exclude_unset=True).items():
#         if value is not None and field not in ['northeast', 'northwest', 'southeast', 'southwest']:
#             query += f' AND mp."{field}" = :{field}'
#             params[field] = value
    
#     # Geospatial filter (bounding box for latitude and longitude)
#     if search.northeast and search.southwest:
#         # Extract latitude and longitude from coordinates
#         northeast_latitude = search.northeast.latitude
#         northeast_longitude = search.northeast.longitude
#         southwest_latitude = search.southwest.latitude
#         southwest_longitude = search.southwest.longitude
        
#         query += ' AND CAST(mp."Latitude" AS FLOAT) BETWEEN :southwest_latitude AND :northeast_latitude'
#         query += ' AND CAST(mp."Longitude" AS FLOAT) BETWEEN :southwest_longitude AND :northeast_longitude'
        
#         # Update parameters for latitudes and longitudes
#         params['northeast_latitude'] = northeast_latitude
#         params['southwest_latitude'] = southwest_latitude
#         params['northeast_longitude'] = northeast_longitude
#         params['southwest_longitude'] = southwest_longitude

#     # Sort by ListingId
#     query += ' ORDER BY mp."ListingId"'

#     # Pagination
#     offset = (page - 1) * BATCH_SIZE
#     query += ' LIMIT :size OFFSET :offset'
#     params['size'] = BATCH_SIZE
#     params['offset'] = offset

#     # Execute the main query
#     result = db.execute(text(query), params)
#     properties = result.fetchall()

#     # Get total count for pagination
#     count_query = '''
#     SELECT COUNT(*)
#     FROM 
#         property_clusters pc
#     JOIN 
#         merged_property mp ON pc."ListingId" = mp."ListingId"
#     WHERE 
#         pc."StandardStatus" = :status
#     '''
#     for field, value in search.dict(exclude_unset=True).items():
#         if value is not None and field not in ['northeast', 'northwest', 'southeast', 'southwest']:
#             count_query += f' AND mp."{field}" = :{field}'
    
#     # Geospatial filter for count query
#     if search.northeast and search.southwest:
#         count_query += ' AND CAST(mp."Latitude" AS FLOAT) BETWEEN :southwest_latitude AND :northeast_latitude'
#         count_query += ' AND CAST(mp."Longitude" AS FLOAT) BETWEEN :southwest_longitude AND :northeast_longitude'

#     total_count = db.execute(text(count_query), params).scalar()

#     # Raise 404 if no properties found
#     if not properties:
#         raise HTTPException(status_code=404, detail="No properties found")

#     results = []
#     for prop in properties:
#         prop_dict = {column: value for column, value in prop._mapping.items()}

#         # Parse Media (images) and map fields
#         if prop_dict.get('Media'):
#             prop_dict['images'] = [MediaResponse(**item) for item in parse_media(prop_dict['Media'])]
#         else:
#             prop_dict['images'] = []

#         # Update field mappings
#         prop_dict['full_bathrooms'] = prop_dict.pop('BathroomsFull', None)
#         prop_dict['half_bathrooms'] = prop_dict.pop('BathroomsHalf', None)
#         prop_dict['beds'] = prop_dict.pop('BedroomsTotal', None)
#         prop_dict['baths'] = prop_dict.pop('BathroomsTotalInteger', None)
#         prop_dict['latitude'] = prop_dict.pop('Latitude')
#         prop_dict['longitude'] = prop_dict.pop('Longitude')
#         prop_dict['materials'] = prop_dict.pop('ConstructionMaterials', None)
#         prop_dict['type'] = prop_dict.pop('PropertyType', None)
#         prop_dict['address'] = prop_dict.pop('UnparsedAddress', None)
#         prop_dict['price'] = prop_dict.pop('ListPrice', None)

#         # Add new fields that may be missing 
#         prop_dict['appliances'] = None  # Default to None if missing
#         prop_dict['features'] = None
#         prop_dict['attached_garage'] = None
#         prop_dict['size'] = None
#         prop_dict['built_in'] = None
#         prop_dict['description'] = None

#         # Fetch open houses and add to property
#         open_houses = db.query(OpenHouse).filter(OpenHouse.ListingKey == prop_dict.get('ListingKey')).all()
#         prop_dict['open_house_times'] = [OpenHouseResponse(**oh.__dict__) for oh in open_houses]

#         # Add property to results
#         results.append(PropertyResponse(**prop_dict))

#     response = PaginatedResponse(
#         items=results,
#         total=total_count,
#         page=page,
#         size=BATCH_SIZE
#     )

#     return response






# import redis
# import json
# from fastapi import HTTPException
# from sqlalchemy import text

# # Initialize Redis
# r = redis.Redis(host='localhost', port=6379, db=0)


# @app.post("/search", response_model=PaginatedResponse)
# def search_properties(
#     search: PropertySearch, 
#     db: Session = Depends(get_db),
#     page: int = Query(1, ge=1)
# ):
#     # print("Received Payload:", search.dict())
#     # Generate a unique key for the query based on search parameters and pagination
#     query_key = f"search:{json.dumps(search.dict(exclude_unset=True))}:{page}"

#     # Check if cached result exists
#     cached_result = r.get(query_key)
#     if cached_result:
#         return json.loads(cached_result)

#     # Base query
#     # query = 'SELECT * FROM merged_property WHERE 1=1'
#     # params = {}
#     query = 'SELECT * FROM property_clusters WHERE "StandardStatus" = :status'
#     params = {"status": "Active"}
#     # Apply other filters from the search payload (excluding geospatial fields)
#     for field, value in search.dict(exclude_unset=True).items():
#         if value is not None and field not in ['northeast', 'northwest', 'southeast', 'southwest']:
#             query += f' AND "{field}" = :{field}'
#             params[field] = value
    
#     # Geospatial filter (bounding box for latitude and longitude)
#     if search.northeast and search.southwest:
#         # Extract latitude and longitude from coordinates
#         northeast_latitude = search.northeast.latitude
#         northeast_longitude = search.northeast.longitude
#         southwest_latitude = search.southwest.latitude
#         southwest_longitude = search.southwest.longitude
        
#         query += ' AND CAST("Latitude" AS FLOAT) BETWEEN :southwest_latitude AND :northeast_latitude'
#         query += ' AND CAST("Longitude" AS FLOAT) BETWEEN :southwest_longitude AND :northeast_longitude'
        
#         # Update parameters for latitudes and longitudes
#         params['northeast_latitude'] = northeast_latitude
#         params['southwest_latitude'] = southwest_latitude
#         params['northeast_longitude'] = northeast_longitude
#         params['southwest_longitude'] = southwest_longitude


#     # Sort by ListingId
#     query += ' ORDER BY "ListingId"'

#     # Pagination
#     offset = (page - 1) * BATCH_SIZE
#     query += ' LIMIT :size OFFSET :offset'
#     params['size'] = BATCH_SIZE
#     params['offset'] = offset
#     # print(query)
#     # Execute the main query
#     result = db.execute(text(query), params)
#     properties = result.fetchall()

#     # Get total count for pagination
#     count_query = 'SELECT COUNT(*) FROM merged_property WHERE 1=1'
#     for field, value in search.dict(exclude_unset=True).items():
#         if value is not None and field not in ['northeast', 'northwest', 'southeast', 'southwest']:
#             count_query += f' AND "{field}" = :{field}'
    
#     # Geospatial filter for count query
#     if search.northeast and search.southwest:
#         count_query += ' AND CAST("Latitude" AS FLOAT) BETWEEN :southwest_latitude AND :northeast_latitude'
#         count_query += ' AND  CAST("Longitude" AS FLOAT) BETWEEN :southwest_longitude AND :northeast_longitude'

#     total_count = db.execute(text(count_query), params).scalar()

#     # Raise 404 if no properties found
#     if not properties:
#         raise HTTPException(status_code=404, detail="No properties found")

#     results = []
#     for prop in properties:
#         prop_dict = {column: value for column, value in prop._mapping.items()}

#         # Parse Media (images) and map fields
#         if prop_dict.get('Media'):
#             prop_dict['images'] = [MediaResponse(**item) for item in parse_media(prop_dict['Media'])]
#         else:
#             prop_dict['images'] = []

#         # Update field mappings
#         prop_dict['full_bathrooms'] = prop_dict.pop('BathroomsFull', None)
#         prop_dict['half_bathrooms'] = prop_dict.pop('BathroomsHalf', None)
#         prop_dict['beds'] = prop_dict.pop('BedroomsTotal', None)
#         prop_dict['baths'] = prop_dict.pop('BathroomsTotalInteger', None)
#         prop_dict['latitude'] = prop_dict.pop('Latitude')
#         prop_dict['longitude'] = prop_dict.pop('Longitude')
#         prop_dict['materials'] = prop_dict.pop('ConstructionMaterials', None)
#         prop_dict['type'] = prop_dict.pop('PropertyType', None)
#         prop_dict['address'] = prop_dict.pop('UnparsedAddress', None)
#         prop_dict['price'] = prop_dict.pop('ListPrice', None)


#         # Add new fields that may be missing 
#         prop_dict['appliances'] = None  # Default to None if missing
#         prop_dict['features'] = None
#         prop_dict['attached_garage'] = None
#         prop_dict['size'] = None
#         prop_dict['built_in'] = None
#         # prop_dict['price'] = None
#         prop_dict['description'] = None

#         # Fetch open houses and add to property
#         open_houses = db.query(OpenHouse).filter(OpenHouse.ListingKey == prop_dict.get('ListingKey')).all()
#         prop_dict['open_house_times'] = [OpenHouseResponse(**oh.__dict__) for oh in open_houses]

#         # Add property to results
#         results.append(PropertyResponse(**prop_dict))

#     response = PaginatedResponse(
#         items=results,
#         total=total_count,
#         page=page,
#         size=BATCH_SIZE
#     )

#     # # Cache the result in Redis for future queries (set to expire after 10 minutes)
#     r.set(query_key, json.dumps(response.dict()), ex=600)

#     return response



# # ###cluster property data ############

# import redis
# import json
# from fastapi import FastAPI, Depends, HTTPException
# from sqlalchemy.orm import Session
# from database import get_db
# from typing import List, Dict
# from pydantic import BaseModel
# from sqlalchemy import text, cast, Float

# # Initialize Redis
# r = redis.Redis(host='localhost', port=6379, db=0)

# class Coordinate(BaseModel):
#     latitude: float
#     longitude: float

# class MapFocus(BaseModel):
#     northeast: Coordinate
#     northwest: Coordinate
#     southeast: Coordinate
#     southwest: Coordinate

# class PropertyDetails(BaseModel):
#     Latitude: float
#     Longitude: float
#     ListingId: str 
#     property_id: str 

# class ClusteringResponse(BaseModel):
#     clusters: List[Dict]  

# @app.post("/cluster_properties_data", response_model=ClusteringResponse)
# def cluster_properties(
#     map_focus: MapFocus,
#     db: Session = Depends(get_db),
# ):
#     # Generate a unique cache key based on map focus coordinates
#     query_key = f"cluster:{map_focus.northeast.latitude},{map_focus.northeast.longitude}:" \
#                 f"{map_focus.southwest.latitude},{map_focus.southwest.longitude}"

#     # # Check if cached result exists in Redis
#     cached_result = r.get(query_key)
#     if cached_result:
#         return ClusteringResponse(**json.loads(cached_result))

#     # Extract map focus coordinates
#     north_east_latitude = map_focus.northeast.latitude
#     north_east_longitude = map_focus.northeast.longitude
#     south_west_latitude = map_focus.southwest.latitude
#     south_west_longitude = map_focus.southwest.longitude

#     # Query to cast Latitude and Longitude as Float
#     properties = db.query(
#         cast(Property.Latitude, Float),
#         cast(Property.Longitude, Float),
#         Property.ListingId,
#         Property.ListPrice
#     ).filter(
#         cast(Property.Latitude, Float).between(south_west_latitude, north_east_latitude),
#         cast(Property.Longitude, Float).between(south_west_longitude, north_east_longitude),
#         Property.StandardStatus.in_(["Active", "Pending", "Coming Soon"])
#     ).all()

#     if not properties:
#         raise HTTPException(status_code=404, detail="No properties found in this area")

#     # Process the query result
#     response_clusters = []

#     # Here, we only add property details (without 'location' and 'count')
#     for prop in properties:
#         response_clusters.append({
#             "latitude": float(prop[0]),
#             "longitude": float(prop[1]),
#             "listingId": str(prop[2]),
#             "price": str(prop[3])
#         })

#     # Prepare the final response
#     response = ClusteringResponse(clusters=response_clusters)

#     # Cache the result in Redis with an expiration time of 10 minutes (600 seconds)
#     r.set(query_key, json.dumps(response.dict()), ex=600)

#     return response






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
    longitude:float

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

    # Check if cached result exists in Redis
    cached_result = r.get(query_key)
    if cached_result:
        return ClusteringResponse(**json.loads(cached_result))

    # Extract map focus coordinates
    north_east_latitude = map_focus.northeast.latitude
    north_east_longitude = map_focus.northeast.longitude
    south_west_latitude = map_focus.southwest.latitude
    south_west_longitude = map_focus.southwest.longitude

    # Query the property_cluster table
    query = text("""
        SELECT "Latitude", "Longitude", "ListingId", "ListPrice" 
        FROM property_cluster
        WHERE CAST("Latitude" AS FLOAT) BETWEEN :south_west_latitude AND :north_east_latitude
        AND CAST("Longitude" AS FLOAT) BETWEEN :south_west_longitude AND :north_east_longitude
        AND "StandardStatus" IN ('Active', 'Pending', 'Coming Soon')
    """)
    
    params = {
        "south_west_latitude": south_west_latitude,
        "north_east_latitude": north_east_latitude,
        "south_west_longitude": south_west_longitude,
        "north_east_longitude": north_east_longitude
    }
    result = db.execute(query, params)
    properties = result.fetchall()

    if not properties:
        raise HTTPException(status_code=404, detail="No properties found in this area with the specified statuses")

    # Process the query result
    response_clusters = []

    for prop in properties:
        response_clusters.append({
            "latitude": float(prop.Latitude),
            "longitude": float(prop.Longitude),
            "listingId": str(prop.ListingId),
            "price": str(prop.ListPrice)
        })

    # Prepare the final response
    response = ClusteringResponse(clusters=response_clusters)

    # Cache the result in Redis with an expiration time of 10 minutes (600 seconds)
    r.set(query_key, json.dumps(response.dict()), ex=600)

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
        SELECT "Latitude", "Longitude" FROM property_cluster 
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




from sqlalchemy import Column, Integer, Float, String, Numeric, Text , Boolean
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session
from fastapi import FastAPI, Depends, HTTPException
from pydantic import BaseModel
from typing import List, Optional
from database import engine, get_db  # Assuming you have a database module

Base = declarative_base()



    

# # Ensure table creation
# Base.metadata.create_all(bind=engine)

# Pydantic model for API response
class PropertyClusterResponse(BaseModel):
    id: int
    Latitude: Optional[float]
    Longitude: Optional[float]
    ListingId: Optional[str]
    ListPrice: Optional[float]
    StandardStatus: Optional[str]

    # class Config:
    #     orm_mode = True

from sqlalchemy import text
from sqlalchemy import Column, Integer, Float, String, Numeric

class PropertyCluster(Base):
    __tablename__ = 'property_cluster'
    
    id = Column(Integer, primary_key=True, index=True)
    Latitude = Column(Float)
    Longitude = Column(Float)
    ListingId = Column(String)
    ListPrice = Column(Numeric)
    StandardStatus = Column(String)
    
    # New fields based on prop_dict
    AboveGradeFinishedArea = Column(String, nullable=True)
    AccessibilityFeatures = Column(String, nullable=True)
    AdditionalParcelsDescription = Column(String, nullable=True)
    AdditionalParcelsYN = Column(String, nullable=True)
    Appliances = Column(String, nullable=True)
    ArchitecturalStyle = Column(String, nullable=True)
    AssociationAmenities = Column(String, nullable=True)
    AssociationFee = Column(String, nullable=True)
    AssociationFee2 = Column(String, nullable=True)
    AssociationFee2Frequency = Column(String, nullable=True)
    AssociationFeeFrequency = Column(String, nullable=True)
    AssociationFeeIncludes = Column(String, nullable=True)
    AssociationName = Column(String, nullable=True)
    AssociationName2 = Column(String, nullable=True)
    AssociationPhone = Column(String, nullable=True)
    AssociationPhone2 = Column(String, nullable=True)
    AssociationYN = Column(String, nullable=True)
    AttachedGarageYN = Column(String, nullable=True)
    AvailabilityDate = Column(String, nullable=True)
    Basement = Column(String, nullable=True)
    BasementYN = Column(String, nullable=True)
    BathroomsFull = Column(Integer, nullable=True)
    BathroomsHalf = Column(Integer, nullable=True)
    BathroomsOneQuarter = Column(String, nullable=True)
    BathroomsThreeQuarter = Column(String, nullable=True)
    BathroomsTotalInteger = Column(Integer, nullable=True)
    BedroomsTotal = Column(Integer, nullable=True)
    BelowGradeFinishedArea = Column(String, nullable=True)
    BodyType = Column(String, nullable=True)
    BuilderModel = Column(String, nullable=True)
    BuilderName = Column(String, nullable=True)
    BuildingAreaSource = Column(String, nullable=True)
    BuildingAreaTotal = Column(String, nullable=True)
    BuildingAreaUnits = Column(String, nullable=True)
    BuildingFeatures = Column(String, nullable=True)
    BusinessName = Column(String, nullable=True)
    BusinessType = Column(String, nullable=True)
    BuyerAgentAOR = Column(String, nullable=True)
    BuyerAgentDirectPhone = Column(String, nullable=True)
    BuyerAgentFullName = Column(String, nullable=True)
    BuyerAgentKey = Column(String, nullable=True)
    BuyerAgentMlsId = Column(String, nullable=True)
    BuyerFinancing = Column(String, nullable=True)
    BuyerOfficeKey = Column(String, nullable=True)
    BuyerOfficeMlsId = Column(String, nullable=True)
    BuyerOfficeName = Column(String, nullable=True)
    BuyerOfficePhone = Column(String, nullable=True)
    BuyerTeamKey = Column(String, nullable=True)
    BuyerTeamKeyNumeric = Column(String, nullable=True)
    BuyerTeamName = Column(String, nullable=True)
    CarportYN = Column(String, nullable=True)
    City = Column(String, nullable=True)
    CloseDate = Column(String, nullable=True)
    ClosePrice = Column(String, nullable=True)
    CoBuyerAgentDirectPhone = Column(String, nullable=True)
    CoBuyerAgentEmail = Column(String, nullable=True)
    CoBuyerAgentFullName = Column(String, nullable=True)
    CoBuyerAgentKey = Column(String, nullable=True)
    CoBuyerAgentMlsId = Column(String, nullable=True)
    CoBuyerOfficeKey = Column(String, nullable=True)
    CoBuyerOfficeMlsId = Column(String, nullable=True)
    CoBuyerOfficeName = Column(String, nullable=True)
    CoBuyerOfficePhone = Column(String, nullable=True)
    CoListAgentDirectPhone = Column(String, nullable=True)
    CoListAgentEmail = Column(String, nullable=True)
    CoListAgentFullName = Column(String, nullable=True)
    CoListAgentKey = Column(String, nullable=True)
    CoListAgentMlsId = Column(String, nullable=True)
    CoListOfficeKey = Column(String, nullable=True)
    CoListOfficeMlsId = Column(String, nullable=True)
    CoListOfficeName = Column(String, nullable=True)
    CoListOfficePhone = Column(String, nullable=True)
    CommunityFeatures = Column(String, nullable=True)
    ConcessionsAmount = Column(String, nullable=True)
    ConstructionMaterials = Column(String, nullable=True)
    Contingency = Column(String, nullable=True)
    Cooling = Column(String, nullable=True)
    Country = Column(String, nullable=True)
    CountyOrParish = Column(String, nullable=True)
    CrossStreet = Column(String, nullable=True)
    CultivatedArea = Column(String, nullable=True)
    CumulativeDaysOnMarket = Column(String, nullable=True)
    CurrentUse = Column(String, nullable=True)
    DaysOnMarket = Column(String, nullable=True)
    DevelopmentStatus = Column(String, nullable=True)
    DirectionFaces = Column(String, nullable=True)
    Directions = Column(String, nullable=True)
    Disclosures = Column(String, nullable=True)
    DocumentsAvailable = Column(String, nullable=True)
    DocumentsChangeTimestamp = Column(String, nullable=True)
    DocumentsCount = Column(String, nullable=True)
    Electric = Column(String, nullable=True)
    ElectricExpense = Column(String, nullable=True)
    ElementarySchool = Column(String, nullable=True)
    ExpirationDate = Column(String, nullable=True)
    ExteriorFeatures = Column(String, nullable=True)
    Fencing = Column(String, nullable=True)
    FinancialDataSource = Column(String, nullable=True)
    FireplaceFeatures = Column(String, nullable=True)
    FireplaceYN = Column(String, nullable=True)
    FireplacesTotal = Column(String, nullable=True)
    Flooring = Column(String, nullable=True)
    FoundationArea = Column(String, nullable=True)
    FoundationDetails = Column(String, nullable=True)
    FuelExpense = Column(String, nullable=True)
    Furnished = Column(String, nullable=True)
    GarageSpaces = Column(String, nullable=True)
    GarageYN = Column(String, nullable=True)
    GreenBuildingVerification = Column(String, nullable=True)
    GreenBuildingVerificationType = Column(String, nullable=True)
    GreenEnergyEfficient = Column(String, nullable=True)
    GreenEnergyGeneration = Column(String, nullable=True)
    GreenIndoorAirQuality = Column(String, nullable=True)
    GreenSustainability = Column(String, nullable=True)
    GreenWaterConservation = Column(String, nullable=True)
    GrossIncome = Column(String, nullable=True)
    GrossScheduledIncome = Column(String, nullable=True)
    Heating = Column(String, nullable=True)
    HighSchool = Column(String, nullable=True)
    HighSchoolDistrict = Column(String, nullable=True)
    HomeWarrantyYN = Column(String, nullable=True)
    HorseAmenities = Column(String, nullable=True)
    Inclusions = Column(String, nullable=True)
    InsuranceExpense = Column(String, nullable=True)
    InteriorFeatures = Column(String, nullable=True)
    InternetAddressDisplayYN = Column(String, nullable=True)
    InternetAutomatedValuationDisplayYN = Column(String, nullable=True)
    InternetConsumerCommentYN = Column(String, nullable=True)
    InternetEntireListingDisplayYN = Column(String, nullable=True)
    IrrigationSource = Column(String, nullable=True)
    LandLeaseAmount = Column(String, nullable=True)
    LandLeaseYN = Column(String, nullable=True)
    Latitude = Column(Float)  # Changed back to Float as before
    LaundryFeatures = Column(String, nullable=True)
    LeasableArea = Column(String, nullable=True)
    LeasableAreaUnits = Column(String, nullable=True)
    LeaseAmountFrequency = Column(String, nullable=True)
    LeaseTerm = Column(String, nullable=True)
    Levels = Column(String, nullable=True)
    ListAOR = Column(String, nullable=True)
    ListAgentAOR = Column(String, nullable=True)
    ListAgentDirectPhone = Column(String, nullable=True)
    ListAgentEmail = Column(String, nullable=True)
    ListAgentFax = Column(String, nullable=True)
    ListAgentFullName = Column(String, nullable=True)
    ListAgentKey = Column(String, nullable=True)
    ListAgentMlsId = Column(String, nullable=True)
    ListAgentOfficePhone = Column(String, nullable=True)
    ListAgentOfficePhoneExt = Column(String, nullable=True)
    ListAgentPager = Column(String, nullable=True)
    ListAgentURL = Column(String, nullable=True)
    ListOfficeFax = Column(String, nullable=True)
    ListOfficeKey = Column(String, nullable=True)
    ListOfficeMlsId = Column(String, nullable=True)
    ListOfficeName = Column(String, nullable=True)
    ListOfficePhone = Column(String, nullable=True)
    ListOfficeURL = Column(String, nullable=True)
    ListPrice = Column(Numeric)  # Changed back to Numeric as before
    ListTeamKey = Column(String, nullable=True)
    ListTeamKeyNumeric = Column(String, nullable=True)
    ListTeamName = Column(String, nullable=True)
    ListingAgreement = Column(String, nullable=True)
    ListingContractDate = Column(String, nullable=True)
    ListingId = Column(String)  # Changed back to String as before
    ListingKey = Column(String, nullable=True)
    ListingService = Column(String, nullable=True)
    ListingTerms = Column(String, nullable=True)
    LivingArea = Column(String, nullable=True)
    LivingAreaSource = Column(String, nullable=True)
    LivingAreaUnits = Column(String, nullable=True)
    LockBoxLocation = Column(String, nullable=True)
    LockBoxType = Column(String, nullable=True)
    LotFeatures = Column(String, nullable=True)
    LotSizeAcres = Column(String, nullable=True)
    LotSizeArea = Column(String, nullable=True)
    LotSizeDimensions = Column(String, nullable=True)
    MaintenanceExpense = Column(String, nullable=True)
    MajorChangeTimestamp = Column(String, nullable=True)
    MajorChangeType = Column(String, nullable=True)
    Media = Column(String, nullable=True)
    MiddleOrJuniorSchool = Column(String, nullable=True)
    NetOperatingIncome = Column(String, nullable=True)
    OtherStructures = Column(String, nullable=True)
    Ownership = Column(String, nullable=True)
    ParkingFeatures = Column(String, nullable=True)
    PetsAllowed = Column(String, nullable=True)
    PoolFeatures = Column(String, nullable=True)
    PoolPrivateYN = Column(String, nullable=True)
    Possession = Column(String, nullable=True)
    PropertyCondition = Column(String, nullable=True)
    PropertySubType = Column(String, nullable=True)
    PropertyType = Column(String, nullable=True)
    PublicRemarks = Column(String, nullable=True)
    Roof = Column(String, nullable=True)
    Sewer = Column(String, nullable=True)
    SpecialListingConditions = Column(String, nullable=True)
    State = Column(String, nullable=True)
    TaxAnnualAmount = Column(String, nullable=True)
    TaxLot = Column(String, nullable=True)
    TaxYear = Column(String, nullable=True)
    TotalActualRent = Column(String, nullable=True)
    UnitNumber = Column(String, nullable=True)
    Utilities = Column(String, nullable=True)
    YearBuilt = Column(String, nullable=True)

    Zoning = Column(String, nullable=True)



@app.post("/create-cluster-table/")
def create_cluster_table(db: Session = Depends(get_db)):
    try:
        # Drop the table and cascade dependent objects
        db.execute(text("DROP TABLE IF EXISTS property_cluster CASCADE"))
        # Create the property_clusters table using the metadata
        Base.metadata.create_all(bind=engine)  # Use this line to create all tables
        return {"message": "property_clusters table created successfully"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error creating table: {str(e)}")



import logging

logger = logging.getLogger(__name__)
# API to run clustering logic
@app.post("/run-clustering/")
def run_clustering(db: Session = Depends(get_db)):
        # Displaying schema information (for debugging)
    schema_clusters_query = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'property_cluster'
        """)
    schema_clusters_result = db.execute(schema_clusters_query).fetchall()
    schema_clusters_info = [{"column_name": row[0], "data_type": row[1]} for row in schema_clusters_result]

    schema_merged_query = text("""
            SELECT column_name, data_type 
            FROM information_schema.columns 
            WHERE table_name = 'merged_property'
        """)
    schema_merged_result = db.execute(schema_merged_query).fetchall()
    schema_merged_info = [{"column_name": row[0], "data_type": row[1]} for row in schema_merged_result]

    try:
    # Clustering SQL query (replace the random cluster value with actual clustering logic)
        # Clustering SQL query (replace the random cluster value with actual clustering logic)
        clustering_sql = text("""
    INSERT INTO property_cluster (
        "Latitude", 
        "Longitude",
        "ListingId", 
        "ListPrice", 
        "StandardStatus", 
        "BathroomsFull", 
        "BathroomsHalf", 
        "BedroomsTotal", 
        "BathroomsTotalInteger", 
        "AboveGradeFinishedArea", 
        "AccessibilityFeatures", 
        "AdditionalParcelsDescription", 
        "AdditionalParcelsYN", 
        "Appliances", 
        "ArchitecturalStyle", 
        "AssociationAmenities", 
        "AssociationFee", 
        "AssociationFee2", 
        "AssociationFee2Frequency", 
        "AssociationFeeFrequency", 
        "AssociationFeeIncludes", 
        "AssociationName", 
        "AssociationName2", 
        "AssociationPhone", 
        "AssociationPhone2", 
        "AssociationYN", 
        "AttachedGarageYN", 
        "AvailabilityDate", 
        "Basement", 
        "BasementYN", 
        "BelowGradeFinishedArea", 
        "BodyType", 
        "BuilderModel", 
        "BuilderName", 
        "BuildingAreaSource", 
        "BuildingAreaTotal", 
        "BuildingAreaUnits", 
        "BuildingFeatures", 
        "BusinessName", 
        "BusinessType", 
        "BuyerAgentAOR", 
        "BuyerAgentDirectPhone", 
        "BuyerAgentFullName", 
        "BuyerAgentKey", 
        "BuyerAgentMlsId", 
        "BuyerFinancing", 
        "BuyerOfficeKey", 
        "BuyerOfficeMlsId", 
        "BuyerOfficeName", 
        "BuyerOfficePhone", 
        "BuyerTeamKey", 
        "BuyerTeamKeyNumeric", 
        "BuyerTeamName", 
        "CarportYN", 
        "City", 
        "CloseDate", 
        "ClosePrice", 
        "CoBuyerAgentDirectPhone", 
        "CoBuyerAgentEmail", 
        "CoBuyerAgentFullName", 
        "CoBuyerAgentKey", 
        "CoBuyerAgentMlsId", 
        "CoBuyerOfficeKey", 
        "CoBuyerOfficeMlsId", 
        "CoBuyerOfficeName", 
        "CoBuyerOfficePhone", 
        "CoListAgentDirectPhone", 
        "CoListAgentEmail", 
        "CoListAgentFullName", 
        "CoListAgentKey", 
        "CoListAgentMlsId", 
        "CoListOfficeKey", 
        "CoListOfficeMlsId", 
        "CoListOfficeName", 
        "CoListOfficePhone", 
        "CommunityFeatures", 
        "ConcessionsAmount", 
        "ConstructionMaterials", 
        "Contingency", 
        "Cooling", 
        "Country", 
        "CountyOrParish", 
        "CrossStreet", 
        "CultivatedArea", 
        "CumulativeDaysOnMarket", 
        "CurrentUse", 
        "DaysOnMarket", 
        "DevelopmentStatus", 
        "DirectionFaces", 
        "Directions", 
        "Disclosures", 
        "DocumentsAvailable", 
        "DocumentsChangeTimestamp", 
        "DocumentsCount", 
        "Electric", 
        "ElectricExpense", 
        "ElementarySchool", 
        "ExpirationDate", 
        "ExteriorFeatures", 
        "Fencing", 
        "FinancialDataSource", 
        "FireplaceFeatures", 
        "FireplaceYN", 
        "FireplacesTotal", 
        "Flooring", 
        "FoundationArea", 
        "FoundationDetails", 
        "FuelExpense", 
        "Furnished", 
        "GarageSpaces", 
        "GarageYN", 
        "GreenBuildingVerification", 
        "GreenBuildingVerificationType", 
        "GreenEnergyEfficient", 
        "GreenEnergyGeneration", 
        "GreenIndoorAirQuality", 
        "GreenSustainability", 
        "GreenWaterConservation", 
        "GrossIncome", 
        "GrossScheduledIncome", 
        "Heating", 
        "HighSchool", 
        "HighSchoolDistrict", 
        "HomeWarrantyYN", 
        "HorseAmenities", 
        "Inclusions", 
        "InsuranceExpense", 
        "InteriorFeatures", 
        "InternetAddressDisplayYN", 
        "InternetAutomatedValuationDisplayYN", 
        "InternetConsumerCommentYN", 
        "InternetEntireListingDisplayYN", 
        "IrrigationSource", 
        "LandLeaseAmount", 
        "LandLeaseYN", 
        "LaundryFeatures", 
        "LeasableArea", 
        "LeasableAreaUnits", 
        "LeaseAmountFrequency", 
        "LeaseTerm", 
        "Levels", 
        "ListAOR", 
        "ListAgentAOR", 
        "ListAgentDirectPhone", 
        "ListAgentEmail", 
        "ListAgentFax", 
        "ListAgentFullName", 
        "ListAgentKey", 
        "ListAgentMlsId", 
        "ListAgentOfficePhone", 
        "ListAgentOfficePhoneExt", 
        "ListAgentPager", 
        "ListAgentURL", 
        "ListOfficeFax", 
        "ListOfficeKey", 
        "ListOfficeMlsId", 
        "ListOfficeName", 
        "ListOfficePhone", 
        "ListOfficeURL", 
        "ListTeamKey", 
        "ListTeamKeyNumeric", 
        "ListTeamName", 
        "ListingAgreement", 
        "ListingContractDate", 
        "ListingKey", 
        "ListingService", 
        "ListingTerms", 
        "LivingArea", 
        "LivingAreaSource", 
        "LivingAreaUnits", 
        "LockBoxLocation", 
        "LockBoxType", 
        "LotFeatures", 
        "LotSizeAcres", 
        "LotSizeArea", 
        "LotSizeDimensions", 
        "MaintenanceExpense", 
        "MajorChangeTimestamp", 
        "MajorChangeType", 
        "Media", 
        "MiddleOrJuniorSchool", 
        "NetOperatingIncome", 
        "OtherStructures", 
        "Ownership", 
        "ParkingFeatures", 
        "PetsAllowed", 
        "PoolFeatures", 
        "PoolPrivateYN", 
        "Possession", 
        "PropertyCondition", 
        "PropertySubType", 
        "PropertyType", 
        "PublicRemarks", 
        "Roof", 
        "Sewer", 
        "SpecialListingConditions", 
        "TaxAnnualAmount", 
        "TaxLot", 
        "TaxYear", 
        "TotalActualRent", 
        "UnitNumber", 
        "Utilities", 
        "YearBuilt", 
        "Zoning"
    )
    SELECT 
        "Latitude"::DOUBLE PRECISION, 
        "Longitude"::DOUBLE PRECISION, 
        "ListingId", 
        "ListPrice"::NUMERIC, 
        "StandardStatus", 
        "BathroomsFull"::INTEGER, 
        "BathroomsHalf"::INTEGER, 
        "BedroomsTotal"::INTEGER, 
        "BathroomsTotalInteger"::INTEGER, 
        "AboveGradeFinishedArea", 
        "AccessibilityFeatures", 
        "AdditionalParcelsDescription", 
        "AdditionalParcelsYN", 
        "Appliances", 
        "ArchitecturalStyle", 
        "AssociationAmenities", 
        "AssociationFee", 
        "AssociationFee2", 
        "AssociationFee2Frequency", 
        "AssociationFeeFrequency", 
        "AssociationFeeIncludes", 
        "AssociationName", 
        "AssociationName2", 
        "AssociationPhone", 
        "AssociationPhone2", 
        "AssociationYN", 
        "AttachedGarageYN", 
        "AvailabilityDate", 
        "Basement", 
        "BasementYN", 
        "BelowGradeFinishedArea", 
        "BodyType", 
        "BuilderModel", 
        "BuilderName", 
        "BuildingAreaSource", 
        "BuildingAreaTotal", 
        "BuildingAreaUnits", 
        "BuildingFeatures", 
        "BusinessName", 
        "BusinessType", 
        "BuyerAgentAOR", 
        "BuyerAgentDirectPhone", 
        "BuyerAgentFullName", 
        "BuyerAgentKey", 
        "BuyerAgentMlsId", 
        "BuyerFinancing", 
        "BuyerOfficeKey", 
        "BuyerOfficeMlsId", 
        "BuyerOfficeName", 
        "BuyerOfficePhone", 
        "BuyerTeamKey", 
        "BuyerTeamKeyNumeric", 
        "BuyerTeamName", 
        "CarportYN", 
        "City", 
        "CloseDate", 
        "ClosePrice", 
        "CoBuyerAgentDirectPhone", 
        "CoBuyerAgentEmail", 
        "CoBuyerAgentFullName", 
        "CoBuyerAgentKey", 
        "CoBuyerAgentMlsId", 
        "CoBuyerOfficeKey", 
        "CoBuyerOfficeMlsId", 
        "CoBuyerOfficeName", 
        "CoBuyerOfficePhone", 
        "CoListAgentDirectPhone", 
        "CoListAgentEmail", 
        "CoListAgentFullName", 
        "CoListAgentKey", 
        "CoListAgentMlsId", 
        "CoListOfficeKey", 
        "CoListOfficeMlsId", 
        "CoListOfficeName", 
        "CoListOfficePhone", 
        "CommunityFeatures", 
        "ConcessionsAmount"::NUMERIC, 
        "ConstructionMaterials", 
        "Contingency", 
        "Cooling", 
        "Country", 
        "CountyOrParish", 
        "CrossStreet", 
        "CultivatedArea", 
        "CumulativeDaysOnMarket", 
        "CurrentUse", 
        "DaysOnMarket", 
        "DevelopmentStatus", 
        "DirectionFaces", 
        "Directions", 
        "Disclosures", 
        "DocumentsAvailable", 
        "DocumentsChangeTimestamp", 
        "DocumentsCount", 
        "Electric", 
        "ElectricExpense", 
        "ElementarySchool", 
        "ExpirationDate", 
        "ExteriorFeatures", 
        "Fencing", 
        "FinancialDataSource", 
        "FireplaceFeatures", 
        "FireplaceYN", 
        "FireplacesTotal", 
        "Flooring", 
        "FoundationArea", 
        "FoundationDetails", 
        "FuelExpense", 
        "Furnished", 
        "GarageSpaces", 
        "GarageYN", 
        "GreenBuildingVerification", 
        "GreenBuildingVerificationType", 
        "GreenEnergyEfficient", 
        "GreenEnergyGeneration", 
        "GreenIndoorAirQuality", 
        "GreenSustainability", 
        "GreenWaterConservation", 
        "GrossIncome", 
        "GrossScheduledIncome", 
        "Heating", 
        "HighSchool", 
        "HighSchoolDistrict", 
        "HomeWarrantyYN", 
        "HorseAmenities", 
        "Inclusions", 
        "InsuranceExpense", 
        "InteriorFeatures", 
        "InternetAddressDisplayYN", 
        "InternetAutomatedValuationDisplayYN", 
        "InternetConsumerCommentYN", 
        "InternetEntireListingDisplayYN", 
        "IrrigationSource", 
        "LandLeaseAmount"::NUMERIC, 
        "LandLeaseYN", 
        "LaundryFeatures", 
        "LeasableArea", 
        "LeasableAreaUnits", 
        "LeaseAmountFrequency", 
        "LeaseTerm", 
        "Levels", 
        "ListAOR", 
        "ListAgentAOR", 
        "ListAgentDirectPhone", 
        "ListAgentEmail", 
        "ListAgentFax", 
        "ListAgentFullName", 
        "ListAgentKey", 
        "ListAgentMlsId", 
        "ListAgentOfficePhone", 
        "ListAgentOfficePhoneExt", 
        "ListAgentPager", 
        "ListAgentURL", 
        "ListOfficeFax", 
        "ListOfficeKey", 
        "ListOfficeMlsId", 
        "ListOfficeName", 
        "ListOfficePhone", 
        "ListOfficeURL", 
        "ListTeamKey", 
        "ListTeamKeyNumeric", 
        "ListTeamName", 
        "ListingAgreement", 
        "ListingContractDate", 
        "ListingKey", 
        "ListingService", 
        "ListingTerms", 
        "LivingArea"::NUMERIC, 
        "LivingAreaSource", 
        "LivingAreaUnits", 
        "LockBoxLocation", 
        "LockBoxType", 
        "LotFeatures", 
        "LotSizeAcres"::NUMERIC, 
        "LotSizeArea", 
        "LotSizeDimensions", 
        "MaintenanceExpense", 
        "MajorChangeTimestamp", 
        "MajorChangeType", 
        "Media", 
        "MiddleOrJuniorSchool", 
        "NetOperatingIncome", 
        "OtherStructures", 
        "Ownership", 
        "ParkingFeatures", 
        "PetsAllowed", 
        "PoolFeatures", 
        "PoolPrivateYN", 
        "Possession", 
        "PropertyCondition", 
        "PropertySubType", 
        "PropertyType", 
        "PublicRemarks", 
        "Roof", 
        "Sewer", 
        "SpecialListingConditions", 
        "TaxAnnualAmount"::NUMERIC, 
        "TaxLot",  
        "TaxYear", 
        "TotalActualRent", 
        "UnitNumber", 
        "Utilities", 
        "YearBuilt"::INTEGER, 
        "Zoning"
    FROM merged_property 
    WHERE "StandardStatus" IN ('Active', 'Pending', 'Coming Soon') ;
    """)

        
            # Execute clustering query
        result = db.execute(clustering_sql)
        db.commit()

        # Log the number of records inserted
        logger.info(f"Inserted {result.rowcount} records into property_cluster.")

        return {
        "message": "Clustering data inserted successfully into property_clusters",
        "inserted_count": result.rowcount,
        "schema_info_clusters": schema_clusters_info,
        "schema_info_merged": schema_merged_info
        }

    except Exception as e:
        db.rollback()
        logger.error(f"Clustering error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"An error occurred during clustering: {str(e)}")

from sqlalchemy import func


# API to fetch all clustered data
@app.get("/clusters/")
def get_clusters(db: Session = Depends(get_db)):
    # Get total count for pagination
    count_query = 'SELECT COUNT(*) FROM property_clusters2 WHERE 1=1'


    total_count = db.execute(text(count_query)).scalar()
    return {"total":total_count}



@app.post("/search1", response_model=PaginatedResponse)
def search_properties1(
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
    # query = 'SELECT * FROM merged_property WHERE 1=1'
    # params = {}
    query = 'SELECT * FROM property_cluster WHERE "StandardStatus" = :status'
    params = {"status": "Active"}
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
    count_query = 'SELECT COUNT(*) FROM property_cluster where 1=1 '

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


    return response









# def parses_media(media_str):
#     try:
#         return json.loads(media_str)  # Parse if media_str is a JSON string
#     except Exception as e:
#         return []



# @app.post("/search3", response_model=PaginatedResponse)
# def search_properties1(
#     search: PropertySearch, 
#     db: Session = Depends(get_db),
#     page: int = Query(1, ge=1)
# ):
#     query_key = f"search:{json.dumps(search.dict(exclude_unset=True))}:{page}"

#     # Check if cached result exists
#     cached_result = r.get(query_key)
#     if cached_result:
#         return json.loads(cached_result)

#     # Base query from materialized view
#     query = '''
#     SELECT * FROM property_joined_view
#     WHERE "StandardStatus" = :status
#     '''
#     params = {"status": "Active"}

#     # Apply filters from the search payload (excluding geospatial fields)
#     for field, value in search.dict(exclude_unset=True).items():
#         if value is not None and field not in ['northeast', 'northwest', 'southeast', 'southwest']:
#             query += f' AND "{field}" = :{field}'
#             params[field] = value

#     # Geospatial filter
#     if search.northeast and search.southwest:
#         northeast_latitude = search.northeast.latitude
#         northeast_longitude = search.northeast.longitude
#         southwest_latitude = search.southwest.latitude
#         southwest_longitude = search.southwest.longitude

#         query += ' AND CAST("Latitude" AS FLOAT) BETWEEN :southwest_latitude AND :northeast_latitude'
#         query += ' AND CAST("Longitude" AS FLOAT) BETWEEN :southwest_longitude AND :northeast_longitude'

#         params['northeast_latitude'] = northeast_latitude
#         params['southwest_latitude'] = southwest_latitude
#         params['northeast_longitude'] = northeast_longitude
#         params['southwest_longitude'] = southwest_longitude

#     # Sort by ListingId and pagination
#     query += ' ORDER BY "ListingId"'
#     offset = (page - 1) * BATCH_SIZE
#     query += ' LIMIT :size OFFSET :offset'
#     params['size'] = BATCH_SIZE
#     params['offset'] = offset

#     # Execute the query
#     result = db.execute(text(query), params)
#     properties = result.fetchall()

#     # Get total count for pagination
#     count_query = 'SELECT COUNT(*) FROM property_joined_view WHERE "StandardStatus" = :status'
#     total_count = db.execute(text(count_query), params).scalar()

#     if not properties:
#         raise HTTPException(status_code=404, detail="No properties found")

#     # Build the response with null defaults for missing fields
#     results = []
#     for prop in properties:
#         prop_dict = {column: value for column, value in prop._mapping.items()}

#         # Map fields and handle null values
#         prop_dict['full_bathrooms'] = prop_dict.pop('BathroomsFull', None)
#         prop_dict['half_bathrooms'] = prop_dict.pop('BathroomsHalf', None)
#         prop_dict['beds'] = prop_dict.pop('BedroomsTotal', None)
#         prop_dict['baths'] = prop_dict.pop('BathroomsTotalInteger', None)
#         prop_dict['latitude'] = prop_dict.pop('Latitude', None)
#         prop_dict['longitude'] = prop_dict.pop('Longitude', None)
#         prop_dict['materials'] = prop_dict.pop('ConstructionMaterials', None)
#         prop_dict['type'] = prop_dict.pop('PropertyType', None)
#         prop_dict['address'] = prop_dict.pop('UnparsedAddress', None)
#         prop_dict['price'] = prop_dict.pop('ListPrice', None)

#         # Add new fields that may be missing and default to None
#         prop_dict['appliances'] = prop_dict.get('Appliances', None)
#         prop_dict['features'] = prop_dict.get('Features', None)
#         prop_dict['attached_garage'] = prop_dict.get('AttachedGarage', None)
#         prop_dict['size'] = prop_dict.get('Size', None)
#         prop_dict['built_in'] = prop_dict.get('BuiltIn', None)
#         prop_dict['description'] = prop_dict.get('PublicRemarks', None)

#         # Parse Media (images) and set null if missing
#         if prop_dict.get('Media'):
#             prop_dict['images'] = [MediaResponse(**item) for item in parses_media(prop_dict['Media'])]
#         else:
#             prop_dict['images'] = []

#         # Fetch open houses and set null if missing
#         open_houses = db.query(OpenHouse).filter(OpenHouse.ListingKey == prop_dict.get('ListingKey')).all()
#         prop_dict['open_house_times'] = [OpenHouseResponse(**oh.__dict__) for oh in open_houses] if open_houses else []

#         # Add property to results
#         results.append(PropertyResponse(**prop_dict))

#     response = PaginatedResponse(
#         items=results,
#         total=total_count,
#         page=page,
#         size=BATCH_SIZE
#     )

#     return response




if __name__ == "__main__":
    uvicorn.run(app, host="192.168.36.100", port=9999)

