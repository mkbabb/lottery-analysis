WITH
    toast
    AS
    (
        SELECT
            *
        FROM
            data
    ),
    pancake
    AS
    (
        SELECT
            toast.MasterSiteID AS id,
            toast.orgName,
            toast.NC18_pdt010needinttech AS answerColumn,
            count(*) AS answerCount
        FROM
            toast
        GROUP BY answerColumn, id
        ORDER BY id, answerColumn ASC
    ),
    crepe
    AS
    (
        SELECT
            id,
            orgName,
            (CASE WHEN answerColumn = 1 THEN answerCount ELSE 0 END) AS t1,
            (CASE WHEN answerColumn = 2 THEN answerCount ELSE 0 END) AS t2,
            (CASE WHEN answerColumn = 3 THEN answerCount ELSE 0 END) AS t3,
            (CASE WHEN answerColumn = 4 THEN answerCount ELSE 0 END) AS t4,
            (CASE WHEN answerColumn = 5 THEN answerCount ELSE 0 END) AS t5,
            (CASE WHEN answerColumn = 6 THEN answerCount ELSE 0 END) AS t6,
            (CASE WHEN answerColumn = 7 THEN answerCount ELSE 0 END) AS t7,
            (CASE WHEN answerColumn = 8 THEN answerCount ELSE 0 END) AS t8
        FROM
            pancake
        ORDER BY id
    ),
    bacon
    AS
    (
        SELECT
            crepe.id,
            crepe.orgName,
            max(t1),
            max(t2),
            max(t3),
            max(t4),
            max(t5),
            max(t6),
            max(t7),
            max(t8),
            max(t1) + max(t2) + max(t3) + max(t4) + max(t5) + max(t6) + max(t7) + max(t8)
        FROM
            crepe
        GROUP BY id
    )
SELECT
    *
FROM
    bacon