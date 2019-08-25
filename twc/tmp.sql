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
            NC18_tmt046collabpln AS answerColumn_0,
            count(*) AS answerCount,
            *
        FROM
            toast
        GROUP BY answerColumn_0, MasterSiteID
    ),
    crepe
    AS
    (
        SELECT
            (CASE WHEN answerColumn_0 = 1 THEN answerCount ELSE 0 END) AS _NC18_tmt046collabpln__None,
            (CASE WHEN answerColumn_0 = 2 THEN answerCount ELSE 0 END) AS _NC18_tmt046collabpln_Less_than_or_equal_to_1_hour,
            (CASE WHEN answerColumn_0 = 3 THEN answerCount ELSE 0 END) AS _NC18_tmt046collabpln_More_than_1_hour_but_less_than_or_equal_to_3_hours,
            (CASE WHEN answerColumn_0 = 4 THEN answerCount ELSE 0 END) AS _NC18_tmt046collabpln_More_than_3_hours_but_less_than_or_equal_to_5_hours,
            (CASE WHEN answerColumn_0 = 5 THEN answerCount ELSE 0 END) AS _NC18_tmt046collabpln_More_than_5_hours_but_less_than_or_equal_to_10_hours,
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
            max(_NC18_tmt046collabpln__None) AS NC18_tmt046collabpln__None,
            max(_NC18_tmt046collabpln_Less_than_or_equal_to_1_hour) AS NC18_tmt046collabpln_Less_than_or_equal_to_1_hour,
            max(_NC18_tmt046collabpln_More_than_1_hour_but_less_than_or_equal_to_3_hours) AS NC18_tmt046collabpln_More_than_1_hour_but_less_than_or_equal_to_3_hours,
            max(_NC18_tmt046collabpln_More_than_3_hours_but_less_than_or_equal_to_5_hours) AS NC18_tmt046collabpln_More_than_3_hours_but_less_than_or_equal_to_5_hours,
            max(_NC18_tmt046collabpln_More_than_5_hours_but_less_than_or_equal_to_10_hours) AS NC18_tmt046collabpln_More_than_5_hours_but_less_than_or_equal_to_10_hours,
            ((max(_NC18_tmt046collabpln__None)+max(_NC18_tmt046collabpln_Less_than_or_equal_to_1_hour)+max(_NC18_tmt046collabpln_More_than_1_hour_but_less_than_or_equal_to_3_hours)+max(_NC18_tmt046collabpln_More_than_3_hours_but_less_than_or_equal_to_5_hours)+max(_NC18_tmt046collabpln_More_than_5_hours_but_less_than_or_equal_to_10_hours)))AS op_NC18_tmt046collabpln
        FROM
            crepe
        GROUP BY MasterSiteID
    )
SELECT
    *
FROM
    bacon