from database import get_db_connection
from schemas import ClaimCreate, ClaimUpdate
from datetime import datetime

#---------------------------------
# Create Claim
#---------------------------------
def create_claim(data: ClaimCreate):

    conn = get_db_connection()
    cursor = conn.cursor()

    query = """
        INSERT INTO claims (policy_id, claim_amount, claim_date, status,description)
        VALUES (%s, %s, %s, %s, %s)
    """

    cursor.execute(query, (
        data.policy_id,
        data.claim_amount,
        data.claim_date,
        data.status,
        data.description
    ))

    conn.commit()
    new_id = cursor.lastrowid

    cursor.close()
    conn.close()
    return new_id

#---------------------------------
# Get All Claims
#---------------------------------
def get_all_claims():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM claims ORDER BY claim_id DESC")
    rows = cursor.fetchall()

    cursor.close()
    conn.close()
    return rows

#---------------------------------
# Get Single Claim
#---------------------------------
def get_claim(claim_id: int):
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    cursor.execute("SELECT * FROM claims WHERE claim_id=%s", (claim_id,))
    row = cursor.fetchone()

    cursor.close()
    conn.close()
    return row

#---------------------------------
# Update Claim
#---------------------------------
def update_claim(claim_id: int, data: ClaimUpdate):
    conn = get_db_connection()
    cursor = conn.cursor()

    fields = []
    values = []

    for key, value in data.dict(exclude_unset=True).items():
        fields.append(f"{key}=%s")
        values.append(value)

    if not fields:
        return False

    values.append(claim_id)

    query = f"UPDATE claims SET {', '.join(fields)} WHERE claim_id=%s"

    cursor.execute(query, tuple(values))
    conn.commit()

    updated = cursor.rowcount > 0

    cursor.close()
    conn.close()
    return updated

#---------------------------------
# Delete Claim
#---------------------------------
def delete_claim(claim_id: int):
    conn = get_db_connection()
    cursor = conn.cursor()

    cursor.execute("DELETE FROM claims WHERE claim_id=%s", (claim_id,))
    conn.commit()

    deleted = cursor.rowcount > 0

    cursor.close()
    conn.close()
    return deleted

def get_policies():
    conn = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT distinct policy_id FROM policies")
    result = cursor.fetchall()
    cursor.close()
    conn.close()
    return [{"id": r[0], "policy_number": r[0]} for r in result]

#---------------------------------
# Dashboard Metrics
#---------------------------------
def get_metrics():
    conn = get_db_connection()
    cursor = conn.cursor(dictionary=True)

    # Total counts by status
    cursor.execute("""
        SELECT status, COUNT(*) AS count
        FROM claims
        GROUP BY status
    """)
    status_rows = cursor.fetchall()

    # Overall total
    cursor.execute("SELECT COUNT(*) AS total FROM claims")
    total = cursor.fetchone()["total"]

    # Monthly trend (last 6 months)
    cursor.execute("""
        SELECT 
            DATE_FORMAT(claim_date, '%Y-%m') AS month,
            COUNT(*) AS count
        FROM claims
        WHERE claim_date >= DATE_SUB(CURDATE(), INTERVAL 6 MONTH)
        GROUP BY month
        ORDER BY month ASC
    """)

    trend_rows = cursor.fetchall()

    cursor.close()
    conn.close()

    # Format metrics
    metrics = {
        "total_claims": total,
        "pending": 0,
        "approved": 0,
        "rejected": 0,
        "monthly_trend": {}
    }

    for r in status_rows:
        status = r["status"].lower()
        if status in metrics:
            metrics[status] = r["count"]

    for row in trend_rows:
        metrics["monthly_trend"][row["month"]] = row["count"]

    return metrics
