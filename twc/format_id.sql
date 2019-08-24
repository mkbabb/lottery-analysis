WITH
    toast
    AS
    (
        SELECT
            *
        FROM
            data
    )

SELECT
    toast.MasterSiteID,
    (
CASE WHEN toast.MasterSiteID LIKE "N%" THEN REPLACE(REPLACE(toast.MasterSiteID, "N", ""), "-", "") ELSE toast.MasterSiteID END) AS formatted_id
FROM
    toast