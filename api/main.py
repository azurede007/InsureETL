from fastapi import FastAPI, HTTPException
from starlette.middleware.cors import CORSMiddleware

from schemas import Claim, ClaimCreate, ClaimUpdate, Metrics
import insureBL

app = FastAPI(
    title="Insurance Claim Management API",
    version="1.0.0"
)
# Allow frontend (HTML/JS) to call API
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)
#---------------------------------
# Create Claim
#---------------------------------
@app.post("/api/claims")
def create_new_claim(data: ClaimCreate):
    print(data)
    new_id = insureBL.create_claim(data)
    return { "claim_id": new_id, **data.dict() }

#---------------------------------
# View All Claims
# http://localhost:8000/api/claims
#---------------------------------
@app.get("/api/claims")
def view_claims():
    return insureBL.get_all_claims()

#---------------------------------
# View Single Claim
#---------------------------------
@app.get("/api/claims/{claim_id}", response_model=Claim)
def get_cgetlaim(claim_id: int):
    claim = insureBL.get_claim(claim_id)
    if not claim:
        raise HTTPException(status_code=404, detail="Claim not found")
    return claim

#---------------------------------
# Update Claim
#---------------------------------
@app.put("/api/claims/{claim_id}", response_model=dict)
def update_claim(claim_id: int, data: ClaimUpdate):
    updated = insureBL.update_claim(claim_id, data)
    if not updated:
        raise HTTPException(status_code=404, detail="Claim not found")
    return {"message": "Claim updated successfully"}

#---------------------------------
# Get Policies
#---------------------------------
@app.get("/api/policy")
def get_policies():
    return insureBL.get_policies()
#---------------------------------
# Delete Claim
#---------------------------------
@app.delete("/api/claims/{claim_id}", response_model=dict)
def delete_claim(claim_id: int):
    deleted = insureBL.delete_claim(claim_id)
    if not deleted:
        raise HTTPException(status_code=404, detail="Claim not found")
    return {"message": "Claim deleted successfully"}


#---------------------------------
# Dashboard Metrics
#---------------------------------
@app.get("/api/metrics", response_model=Metrics)
def get_dashboard_metrics():
    return insureBL.get_metrics()
