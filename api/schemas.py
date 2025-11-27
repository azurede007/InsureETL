from pydantic import BaseModel
from datetime import date
from typing import Optional

class ClaimBase(BaseModel):
    policy_id: int
    claim_amount: float
    claim_date: date
    status: str
    description: str

class ClaimCreate(ClaimBase):
    pass

class ClaimUpdate(BaseModel):
    policy_id: Optional[int]
    claim_amount: Optional[float]
    claim_date: Optional[date]
    status: Optional[str]
    description: Optional[str]

class Claim(ClaimBase):
    claim_id: int

    class Config:
        orm_mode = True

class Metrics(BaseModel):
    total_claims: int
    pending: int
    approved: int
    rejected: int
    monthly_trend: dict
