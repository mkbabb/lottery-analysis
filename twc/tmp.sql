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
            NC18_tmt046collabpln AS answerColumn,
            count(*) AS answerCount,
            *
        FROM
            toast
        GROUP BY answerColumn, MasterSiteID
    ),
    crepe
    AS
    (
        SELECT
            (CASE WHEN answerColumn = 1 THEN answerCount ELSE 0 END) AS __NONE,
            (CASE WHEN answerColumn = 2 THEN answerCount ELSE 0 END) AS _LESS_THAN_OR_EQUAL_TO_1_HOUR,
            (CASE WHEN answerColumn = 3 THEN answerCount ELSE 0 END) AS _MORE_THAN_1_HOUR_BUT_LESS_THAN_OR_EQUAL_TO_3_HOURS,
            (CASE WHEN answerColumn = 4 THEN answerCount ELSE 0 END) AS _MORE_THAN_3_HOURS_BUT_LESS_THAN_OR_EQUAL_TO_5_HOURS,
            (CASE WHEN answerColumn = 5 THEN answerCount ELSE 0 END) AS _MORE_THAN_5_HOURS_BUT_LESS_THAN_OR_EQUAL_TO_10_HOURS,
            *
        FROM
            pancake
    ),
    bacon
    AS
    (
        SELECT
            MasterSiteID,
            orgName,
            max(__NONE) AS _NONE,
            max(_LESS_THAN_OR_EQUAL_TO_1_HOUR) AS LESS_THAN_OR_EQUAL_TO_1_HOUR,
            max(_MORE_THAN_1_HOUR_BUT_LESS_THAN_OR_EQUAL_TO_3_HOURS) AS MORE_THAN_1_HOUR_BUT_LESS_THAN_OR_EQUAL_TO_3_HOURS,
            max(_MORE_THAN_3_HOURS_BUT_LESS_THAN_OR_EQUAL_TO_5_HOURS) AS MORE_THAN_3_HOURS_BUT_LESS_THAN_OR_EQUAL_TO_5_HOURS,
            max(_MORE_THAN_5_HOURS_BUT_LESS_THAN_OR_EQUAL_TO_10_HOURS) AS MORE_THAN_5_HOURS_BUT_LESS_THAN_OR_EQUAL_TO_10_HOURS,
            (max(__NONE)+max(_LESS_THAN_OR_EQUAL_TO_1_HOUR)+max(_MORE_THAN_1_HOUR_BUT_LESS_THAN_OR_EQUAL_TO_3_HOURS)+max(_MORE_THAN_3_HOURS_BUT_LESS_THAN_OR_EQUAL_TO_5_HOURS)+max(_MORE_THAN_5_HOURS_BUT_LESS_THAN_OR_EQUAL_TO_10_HOURS)) AS _total
        FROM
            crepe
        GROUP BY MasterSiteID
    )
SELECT
    *
FROM
    bacon