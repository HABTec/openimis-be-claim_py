import random , string
from sqlalchemy import text
from opensearchpy import helpers

from core.ETLBase import ETLBase

class PatientReferedToAnotherFacilityETL(ETLBase):
    INDEX_NAME = "patient_refered_to_another_facility"

    def process(self):

        offset = 1000
        rows = self._extract(offset)

        transformed = self.transform(rows)
        self.clear_index(self.INDEX_NAME)
        self.load(transformed)


    def _extract(self, offset: int):
        sql = text("""
            WITH billables AS (
                SELECT
                    l."LocationId",
                    l."LocationName",
                    'ITEM' AS billable_type,
                    i."ItemID" AS billable_id,
                    i."ItemName" AS billable_name,
                    c."ValidityFrom" ,
                    COUNT(ci."ClaimItemID") AS frequency,
                    SUM(ci."QtyProvided") AS total_quantity,
                    SUM(ci."PriceAsked") AS total_price,
                    SUM(ci."QtyProvided" * ci."PriceAsked") AS total_cost_etb
                FROM "tblClaimItems" ci
                JOIN "tblItems" i
                    ON i."ItemID" = ci."ItemID"
                AND i."ValidityTo" IS NULL
                JOIN "tblClaim" c
                    ON c."ClaimID" = ci."ClaimID"
                AND c."ValidityTo" IS NULL
                JOIN "tblHF" hf
                    ON hf."HfID" = c."HFID"
                AND hf."ValidityTo" IS NULL
                JOIN "tblLocations" l
                    ON l."LocationId" = hf."LocationId"
                WHERE ci."ValidityTo" IS null and c."ValidityTo" is null
                GROUP BY
                    l."LocationId",
                    l."LocationName",
                    i."ItemID",
                    i."ItemName",
                    c."ValidityFrom" 
                UNION ALL
                SELECT
                    l."LocationId",
                    l."LocationName",
                    'SERVICE' AS billable_type,
                    s."ServiceID" AS billable_id,
                    s."ServName" AS billable_name,
                    c."ValidityFrom" ,
                    COUNT(cs."ClaimServiceID") AS frequency,
                    SUM(cs."QtyProvided") AS total_quantity,
                    SUM(cs."PriceAsked") AS total_price,
                    SUM(cs."QtyProvided" * cs."PriceAsked") AS total_cost_etb
                FROM "tblClaimServices" cs
                JOIN "tblServices" s
                    ON s."ServiceID" = cs."ServiceID"
                AND s."ValidityTo" IS NULL
                JOIN "tblClaim" c
                    ON c."ClaimID" = cs."ClaimID"
                AND c."ValidityTo" IS NULL
                JOIN "tblHF" hf
                    ON hf."HfID" = c."HFID"
                AND hf."ValidityTo" IS NULL
                JOIN "tblLocations" l
                    ON l."LocationId" = hf."LocationId"
                WHERE cs."ValidityTo" IS null and c."ValidityTo" is null
                GROUP BY
                    l."LocationId",
                    l."LocationName",
                    s."ServiceID",
                    s."ServName",
                    c."ValidityFrom" 
            )
            SELECT * , EXTRACT(MONTH FROM to_ethiopian_date("ValidityFrom"::DATE)::date) AS month ,
                    EXTRACT(YEAR FROM to_ethiopian_date("ValidityFrom"::DATE)::date) AS year
            FROM billables
            ORDER BY
                "LocationName",
                total_cost_etb DESC
            LIMIT 10;


        """)
        conn = self.engine.connect()
        result = conn.execute(
            sql,
            # {
            #     "limit": self.BATCH_SIZE,
            #     "offset": offset,
            # }
        )
        res = result.mappings().all()
        return res
     
    def transform(self, rows):
        docs = []

        for row in rows:
            docs.append({
                "_index": self.INDEX_NAME,
                "_source": {
                    "location_id": row.LocationId,
                    "location_name": row.LocationName,
                    "billable_type": row.billable_type,
                    "billable_id": row.billable_id,
                    "billable_name": row.billable_name,
                    "frequency": row.frequency,
                    "total_quantity": row.total_quantity,
                    "total_price": float(row.total_price),
                    "total_cost_etb": float(row.total_cost_etb),
                    "month": int(row.month),
                    "year": int(row.year),
                }
            })

        return docs

    def load(self, docs):
        if not docs:
            return

        success, failed = helpers.bulk(
            client=self.opensearch,
            actions=docs,
            stats_only=True,
        )

        print("Success:", success)
        print("Failed:", failed)
