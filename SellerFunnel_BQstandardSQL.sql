--Seller Funnel is defined to be consisted of 10 steps
WITH
  t1 AS (
  SELECT
    DISTINCT ga_master_table.full_visitor_id AS ga_master_table_full_visitor_id
  FROM
    `batdongsan-datalake-v0.derived.ga_master_nested` AS ga_master_table
  LEFT JOIN
    UNNEST(ga_master_table.hits) AS ga_master_table_hits
  LEFT JOIN
    `batdongsan-datalake-v0.dwh.dim_categories` AS dim_categories
  ON
    (CAST(dim_categories.CateId AS STRING)) = ga_master_table_hits.custom_page_cate_id
  LEFT JOIN
    `batdongsan-datalake-v0.derived.bot_session_detail` AS bot_session_detail
  ON
    bot_session_detail.session_id = CONCAT(ga_master_table.unique_intraaday_id,ga_master_table.visit_date)
  WHERE
    ((( ga_master_table.visit_date ) >= ((DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY)))
        AND ( ga_master_table.visit_date ) < ((DATE_ADD(DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY), INTERVAL 7 DAY)))))
    AND (CASE
        WHEN bot_session_detail.session_id IS NOT NULL THEN 'yes'
      ELSE
      'no'
    END
      ) = 'no'
    AND ga_master_table_hits.event_action = 'Click-Sign Up Button'),
  t2 AS (
  SELECT
    DISTINCT ga_master_table.full_visitor_id AS ga_master_table_full_visitor_id
  FROM
    `batdongsan-datalake-v0.derived.ga_master_nested` AS ga_master_table
  LEFT JOIN
    UNNEST(ga_master_table.hits) AS ga_master_table_hits
  LEFT JOIN
    `batdongsan-datalake-v0.dwh.dim_categories` AS dim_categories
  ON
    (CAST(dim_categories.CateId AS STRING)) = ga_master_table_hits.custom_page_cate_id
  LEFT JOIN
    `batdongsan-datalake-v0.derived.bot_session_detail` AS bot_session_detail
  ON
    bot_session_detail.session_id = CONCAT(ga_master_table.unique_intraaday_id,ga_master_table.visit_date)
  WHERE
    ((( ga_master_table.visit_date ) >= ((DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY)))
        AND ( ga_master_table.visit_date ) < ((DATE_ADD(DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY), INTERVAL 7 DAY)))))
    AND (CASE
        WHEN bot_session_detail.session_id IS NOT NULL THEN 'yes'
      ELSE
      'no'
    END
      ) = 'no'
    AND ga_master_table_hits.event_action = 'Submit-Sign Up Form'),
  t3 AS (
  SELECT
    DISTINCT ga_master_table.full_visitor_id AS ga_master_table_full_visitor_id
  FROM
    `batdongsan-datalake-v0.derived.ga_master_nested` AS ga_master_table
  LEFT JOIN
    UNNEST(ga_master_table.hits) AS ga_master_table_hits
  LEFT JOIN
    `batdongsan-datalake-v0.dwh.dim_categories` AS dim_categories
  ON
    (CAST(dim_categories.CateId AS STRING)) = ga_master_table_hits.custom_page_cate_id
  LEFT JOIN
    `batdongsan-datalake-v0.derived.bot_session_detail` AS bot_session_detail
  ON
    bot_session_detail.session_id = CONCAT(ga_master_table.unique_intraaday_id,ga_master_table.visit_date)
  LEFT JOIN
    `batdongsan-datalake-v0.dwh.dim_user` AS dim_uid_cusid
  ON
    ga_master_table_hits.BDS_user_id = CAST(dim_uid_cusid.Id AS string)
  WHERE
    ((( ga_master_table.visit_date ) >= ((DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY)))
        AND ( ga_master_table.visit_date ) < ((DATE_ADD(DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY), INTERVAL 7 DAY)))))
    AND (CASE
        WHEN bot_session_detail.session_id IS NOT NULL THEN 'yes'
      ELSE
      'no'
    END
      ) = 'no'
    AND ga_master_table_hits.event_action = 'Submit-Sign Up Form'
    AND dim_uid_cusid.IsActived IS NOT NULL
    AND dim_uid_cusid.IsActived = TRUE),
  t4 AS (
  SELECT
    DISTINCT ga_master_table.full_visitor_id AS ga_master_table_full_visitor_id
  FROM
    `batdongsan-datalake-v0.derived.ga_master_nested` AS ga_master_table
  LEFT JOIN
    UNNEST(ga_master_table.hits) AS ga_master_table_hits
  LEFT JOIN
    `batdongsan-datalake-v0.dwh.dim_categories` AS dim_categories
  ON
    (CAST(dim_categories.CateId AS STRING)) = ga_master_table_hits.custom_page_cate_id
  LEFT JOIN
    `batdongsan-datalake-v0.derived.bot_session_detail` AS bot_session_detail
  ON
    bot_session_detail.session_id = CONCAT(ga_master_table.unique_intraaday_id,ga_master_table.visit_date)
  LEFT JOIN
    `batdongsan-datalake-v0.dwh.dim_user` AS dim_uid_cusid
  ON
    ga_master_table_hits.BDS_user_id = CAST(dim_uid_cusid.Id AS string)
  WHERE
    ((( ga_master_table.visit_date ) >= ((DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY)))
        AND ( ga_master_table.visit_date ) < ((DATE_ADD(DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY), INTERVAL 7 DAY)))))
    AND (CASE
        WHEN bot_session_detail.session_id IS NOT NULL THEN 'yes'
      ELSE
      'no'
    END
      ) = 'no'
    AND ga_master_table_hits.event_action = 'Submit-Sign Up Form'
    AND dim_uid_cusid.MobileConfirmDate IS NOT NULL),
  t5 AS (
  SELECT
    DISTINCT ga_master_table.full_visitor_id AS ga_master_table_full_visitor_id
  FROM
    `batdongsan-datalake-v0.derived.ga_master_nested` AS ga_master_table
  LEFT JOIN
    UNNEST(ga_master_table.hits) AS ga_master_table_hits
  LEFT JOIN
    `batdongsan-datalake-v0.dwh.dim_categories` AS dim_categories
  ON
    (CAST(dim_categories.CateId AS STRING)) = ga_master_table_hits.custom_page_cate_id
  LEFT JOIN
    `batdongsan-datalake-v0.derived.bot_session_detail` AS bot_session_detail
  ON
    bot_session_detail.session_id = CONCAT(ga_master_table.unique_intraaday_id,ga_master_table.visit_date)
  WHERE
    ((( ga_master_table.visit_date ) >= ((DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY)))
        AND ( ga_master_table.visit_date ) < ((DATE_ADD(DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY), INTERVAL 7 DAY)))))
    AND (CASE
        WHEN bot_session_detail.session_id IS NOT NULL THEN 'yes'
      ELSE
      'no'
    END
      ) = 'no'
    AND ga_master_table_hits.event_action IN ('Click-Sign In Button',
      'Sign In')),
  t6 AS (
  SELECT
    DISTINCT ga_master_table.full_visitor_id AS ga_master_table_full_visitor_id
  FROM
    `batdongsan-datalake-v0.derived.ga_master_nested` AS ga_master_table
  LEFT JOIN
    UNNEST(ga_master_table.hits) AS ga_master_table_hits
  LEFT JOIN
    `batdongsan-datalake-v0.dwh.dim_categories` AS dim_categories
  ON
    (CAST(dim_categories.CateId AS STRING)) = ga_master_table_hits.custom_page_cate_id
  LEFT JOIN
    `batdongsan-datalake-v0.derived.bot_session_detail` AS bot_session_detail
  ON
    bot_session_detail.session_id = CONCAT(ga_master_table.unique_intraaday_id,ga_master_table.visit_date)
  WHERE
    ((( ga_master_table.visit_date ) >= ((DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY)))
        AND ( ga_master_table.visit_date ) < ((DATE_ADD(DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY), INTERVAL 7 DAY)))))
    AND (CASE
        WHEN bot_session_detail.session_id IS NOT NULL THEN 'yes'
      ELSE
      'no'
    END
      ) = 'no'
    AND ga_master_table_hits.hits_pagepath = '/sellernet/quan-ly-tin-rao-ban-cho-thue'
    AND ga_master_table_hits.hit_type = 'PAGE'),
  t7 AS (
  SELECT
    DISTINCT ga_master_table.full_visitor_id AS ga_master_table_full_visitor_id
  FROM
    `batdongsan-datalake-v0.derived.ga_master_nested` AS ga_master_table
  LEFT JOIN
    UNNEST(ga_master_table.hits) AS ga_master_table_hits
  LEFT JOIN
    `batdongsan-datalake-v0.dwh.dim_categories` AS dim_categories
  ON
    (CAST(dim_categories.CateId AS STRING)) = ga_master_table_hits.custom_page_cate_id
  LEFT JOIN
    `batdongsan-datalake-v0.derived.bot_session_detail` AS bot_session_detail
  ON
    bot_session_detail.session_id = CONCAT(ga_master_table.unique_intraaday_id,ga_master_table.visit_date)
  WHERE
    ((( ga_master_table.visit_date ) >= ((DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY)))
        AND ( ga_master_table.visit_date ) < ((DATE_ADD(DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY), INTERVAL 7 DAY)))))
    AND (CASE
        WHEN bot_session_detail.session_id IS NOT NULL THEN 'yes'
      ELSE
      'no'
    END
      ) = 'no'
    AND ga_master_table_hits.event_action LIKE '%Click-Post Listing%'),
  t8 AS (
  SELECT
    DISTINCT ga_master_table.full_visitor_id AS ga_master_table_full_visitor_id
  FROM
    `batdongsan-datalake-v0.derived.ga_master_nested` AS ga_master_table
  LEFT JOIN
    UNNEST(ga_master_table.hits) AS ga_master_table_hits
  LEFT JOIN
    `batdongsan-datalake-v0.dwh.dim_categories` AS dim_categories
  ON
    (CAST(dim_categories.CateId AS STRING)) = ga_master_table_hits.custom_page_cate_id
  LEFT JOIN
    `batdongsan-datalake-v0.derived.bot_session_detail` AS bot_session_detail
  ON
    bot_session_detail.session_id = CONCAT(ga_master_table.unique_intraaday_id,ga_master_table.visit_date)
  WHERE
    ((( ga_master_table.visit_date ) >= ((DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY)))
        AND ( ga_master_table.visit_date ) < ((DATE_ADD(DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY), INTERVAL 7 DAY)))))
    AND (CASE
        WHEN bot_session_detail.session_id IS NOT NULL THEN 'yes'
      ELSE
      'no'
    END
      ) = 'no'
    AND ga_master_table_hits.event_action = 'Click-Submit'),
  t9 AS (
  SELECT
    DISTINCT ga_master_table.full_visitor_id AS ga_master_table_full_visitor_id
  FROM
    `batdongsan-datalake-v0.derived.ga_master_nested` AS ga_master_table
  LEFT JOIN
    UNNEST(ga_master_table.hits) AS ga_master_table_hits
  LEFT JOIN
    `batdongsan-datalake-v0.dwh.dim_categories` AS dim_categories
  ON
    (CAST(dim_categories.CateId AS STRING)) = ga_master_table_hits.custom_page_cate_id
  LEFT JOIN
    `batdongsan-datalake-v0.derived.bot_session_detail` AS bot_session_detail
  ON
    bot_session_detail.session_id = CONCAT(ga_master_table.unique_intraaday_id,ga_master_table.visit_date)
  WHERE
    ((( ga_master_table.visit_date ) >= ((DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY)))
        AND ( ga_master_table.visit_date ) < ((DATE_ADD(DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY), INTERVAL 7 DAY)))))
    AND (CASE
        WHEN bot_session_detail.session_id IS NOT NULL THEN 'yes'
      ELSE
      'no'
    END
      ) = 'no'
    AND ga_master_table_hits.event_action = 'Click-Check Out'),
  t10 AS (
  SELECT
    DISTINCT ga_master_table.full_visitor_id AS ga_master_table_full_visitor_id
  FROM
    `batdongsan-datalake-v0.derived.ga_master_nested` AS ga_master_table
  LEFT JOIN
    UNNEST(ga_master_table.hits) AS ga_master_table_hits
  LEFT JOIN
    `batdongsan-datalake-v0.dwh.dim_categories` AS dim_categories
  ON
    (CAST(dim_categories.CateId AS STRING)) = ga_master_table_hits.custom_page_cate_id
  LEFT JOIN
    `batdongsan-datalake-v0.derived.bot_session_detail` AS bot_session_detail
  ON
    bot_session_detail.session_id = CONCAT(ga_master_table.unique_intraaday_id,ga_master_table.visit_date)
  WHERE
    ((( ga_master_table.visit_date ) >= ((DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY)))
        AND ( ga_master_table.visit_date ) < ((DATE_ADD(DATE_ADD(CURRENT_DATE('Asia/Ho_Chi_Minh'), INTERVAL -7 DAY), INTERVAL 7 DAY)))))
    AND (CASE
        WHEN bot_session_detail.session_id IS NOT NULL THEN 'yes'
      ELSE
      'no'
    END
      ) = 'no'
    AND ga_master_table_hits.event_action = 'Display-Submission Message'
    AND (ga_master_table_hits.event_label NOT LIKE '%không đủ%'
      OR ga_master_table_hits.event_label NOT LIKE '%fail%'
      OR ga_master_table_hits.event_label NOT LIKE '%thất bại%')),
  sub_total AS (
  SELECT
    t1.ga_master_table_full_visitor_id AS click_signup,
    CASE
      WHEN t2.ga_master_table_full_visitor_id IS NOT NULL AND LENGTH(t2.ga_master_table_full_visitor_id) <> 0 THEN 1
    ELSE
    0
  END
    AS submit_signup,
    CASE
      WHEN t3.ga_master_table_full_visitor_id IS NOT NULL AND LENGTH(t3.ga_master_table_full_visitor_id) <> 0 THEN 1
    ELSE
    0
  END
    AS activate,
    CASE
      WHEN t4.ga_master_table_full_visitor_id IS NOT NULL AND LENGTH(t4.ga_master_table_full_visitor_id) <> 0 THEN 1
    ELSE
    0
  END
    AS mobile_confirm,
    CASE
      WHEN t5.ga_master_table_full_visitor_id IS NOT NULL AND LENGTH(t5.ga_master_table_full_visitor_id) <> 0 THEN 1
    ELSE
    0
  END
    AS click_login,
    CASE
      WHEN t6.ga_master_table_full_visitor_id IS NOT NULL AND LENGTH(t6.ga_master_table_full_visitor_id) <> 0 THEN 1
    ELSE
    0
  END
    AS login,
    CASE
      WHEN t7.ga_master_table_full_visitor_id IS NOT NULL AND LENGTH(t7.ga_master_table_full_visitor_id) <> 0 THEN 1
    ELSE
    0
  END
    AS compile_listing,
    CASE
      WHEN t8.ga_master_table_full_visitor_id IS NOT NULL AND LENGTH(t8.ga_master_table_full_visitor_id) <> 0 THEN 1
    ELSE
    0
  END
    AS submit_listing,
    CASE
      WHEN t9.ga_master_table_full_visitor_id IS NOT NULL AND LENGTH(t9.ga_master_table_full_visitor_id) <> 0 THEN 1
    ELSE
    0
  END
    AS checkout,
    CASE
      WHEN t10.ga_master_table_full_visitor_id IS NOT NULL AND LENGTH(t10.ga_master_table_full_visitor_id) <> 0 THEN 1
    ELSE
    0
  END
    AS checkout_success
  FROM
    t1
  LEFT JOIN
    t2
  ON
    t1.ga_master_table_full_visitor_id = t2.ga_master_table_full_visitor_id
  LEFT JOIN
    t3
  ON
    t1.ga_master_table_full_visitor_id = t3.ga_master_table_full_visitor_id
  LEFT JOIN
    t4
  ON
    t1.ga_master_table_full_visitor_id = t4.ga_master_table_full_visitor_id
  LEFT JOIN
    t5
  ON
    t2.ga_master_table_full_visitor_id = t5.ga_master_table_full_visitor_id
  LEFT JOIN
    t6
  ON
    t2.ga_master_table_full_visitor_id = t6.ga_master_table_full_visitor_id
  LEFT JOIN
    t7
  ON
    t2.ga_master_table_full_visitor_id = t7.ga_master_table_full_visitor_id
  LEFT JOIN
    t8
  ON
    t2.ga_master_table_full_visitor_id = t8.ga_master_table_full_visitor_id
  LEFT JOIN
    t9
  ON
    t2.ga_master_table_full_visitor_id = t9.ga_master_table_full_visitor_id
  LEFT JOIN
    t10
  ON
    t2.ga_master_table_full_visitor_id = t10.ga_master_table_full_visitor_id )
SELECT
  COUNT(DISTINCT sub_total.click_signup) AS click_signup,
  SUM(submit_signup) AS submit_signup,
  SUM(activate) AS activate,
  SUM(mobile_confirm) AS mobile_confirm,
  SUM(click_login) AS click_login,
  SUM(login) AS login,
  SUM(compile_listing) AS compile_listing,
  SUM(submit_listing) AS submit_listing,
  SUM(checkout) AS checkout,
  SUM(checkout_success) AS checkout_success
FROM
  sub_total
