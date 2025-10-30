WITH izv_data AS (
    SELECT DISTINCT
        json_data -> 'izvStartSMR' ->> 'deloNum' AS delo_num,
        json_data -> 'izvStartSMR' -> 'request' ->> 'regDate' AS reg_date,
        json_data -> 'izvStartSMR' -> 'request' ->> 'regNumber' AS reg_number,
        json_data -> 'izvStartSMR' -> 'incomingData' -> 'object' ->> 'objectName' AS object_name,
        json_data -> 'izvStartSMR' -> 'incomingData' -> 'object' -> 'address' -> 0 ->> 'landmarkInfo' AS address_landmark
    FROM izvstartsmr.documents
    WHERE json_data -> 'izvStartSMR' ->> 'deloNum' IS NOT NULL 
),
nadzor_program_files AS (
    SELECT 
        json_data -> 'programCheck' ->> 'deloNum' AS delo_num,
        TRIM(BOTH '"' FROM jsonb_array_elements_text(json_data -> 'programCheck' -> 'programCheckFile' -> 'files')) AS single_file,
        json_data -> 'programCheck' ->> 'approvalDateReal' AS approval_date_real
    FROM nadzor.documents
    WHERE doc_type = 'PROGRAMCHECK'
),
nadzor_program_agg AS (
    SELECT 
        delo_num,
        STRING_AGG(
            '<a href="https:путь' || single_file || 'target="_blank">' || 
            'Программа проверок от ' || TO_CHAR(TO_DATE(approval_date_real, 'YYYY-MM-DD'), 'DD.MM.YYYY') || '</a>', 
            ', ' ORDER BY single_file
        ) AS program_check_files_links,
        STRING_AGG(single_file, ', ' ORDER BY single_file) AS program_check_files_raw,
        MAX(approval_date_real) AS approval_date_real
    FROM nadzor_program_files
    GROUP BY delo_num
),
nadzor_program AS (
    SELECT DISTINCT
        json_data -> 'programCheck' ->> 'deloNum' AS delo_num,
        json_data -> 'programCheck' ->> 'addressObjView' AS address,
        json_data -> 'programCheck' ->> 'objectID' AS objid,
        (SELECT string_agg(
            'с ' || TO_CHAR(TO_DATE(work ->> 'startDate', 'YYYY-MM-DD'), 'DD.MM.YYYY') || ' по ' || 
            TO_CHAR(TO_DATE(work ->> 'endDate', 'YYYY-MM-DD'), 'DD.MM.YYYY'), 
            ', ' ORDER BY (work ->> 'checkWorkNum')::int
         )
         FROM jsonb_array_elements(json_data -> 'programCheck' -> 'Work') AS work
         WHERE work ->> 'event' = 'Завершение выполнения работ, результаты которых подлежат проверке.'
        ) AS expected_dates
    FROM nadzor.documents
    WHERE doc_type = 'PROGRAMCHECK'
),
latest_date_range AS (
    SELECT 
        json_data -> 'programCheck' ->> 'deloNum' AS delo_num,
        'с ' || TO_CHAR(TO_DATE(work ->> 'startDate', 'YYYY-MM-DD'), 'DD.MM.YYYY') || ' по ' || 
        TO_CHAR(TO_DATE(work ->> 'endDate', 'YYYY-MM-DD'), 'DD.MM.YYYY') AS date_range,
        TO_DATE(work ->> 'endDate', 'YYYY-MM-DD') AS end_date
    FROM nadzor.documents,
    LATERAL jsonb_array_elements(json_data -> 'programCheck' -> 'Work') AS work
    WHERE doc_type = 'PROGRAMCHECK'
      AND work ->> 'event' = 'Завершение выполнения работ, результаты которых подлежат проверке.'
),
latest_range_per_delo AS (
    SELECT DISTINCT ON (delo_num)
        delo_num,
        date_range
    FROM latest_date_range
    ORDER BY delo_num, end_date DESC
),
nadzor_program_combined AS (
    SELECT 
        np.delo_num,
        np.address,
        np.objid,
        np.expected_dates,
        npa.program_check_files_links,
        npa.program_check_files_raw,
        npa.approval_date_real,
        lr.date_range AS last_expected_date_range
    FROM nadzor_program np
    LEFT JOIN nadzor_program_agg npa ON np.delo_num = npa.delo_num
    LEFT JOIN latest_range_per_delo lr ON np.delo_num = lr.delo_num
),
rs_data_ungrouped AS (
    SELECT DISTINCT
        json_data -> 'rs' -> 'incomingData' -> 'object' ->> 'deloNum' AS delo_num_object,
        COALESCE(
            json_data -> 'rs' -> 'incomingData' -> 'examination' -> 0 ->> 'examinationNumber', 
            json_data -> 'rs' -> 'rsDocument' -> 'examinationEdit' -> 0 ->> 'examinationNumberEdit'
        ) AS exam_edit_number,
        COALESCE(
            json_data -> 'rs' -> 'rsDocument' ->> 'totalAddress',
            json_data -> 'rs' -> 'passportObject' -> 'address' -> 0 ->> 'landmarkInfo',
            json_data -> 'rs' -> 'incomingData' -> 'object' -> 'constructionProject' ->> 'constructionProjectAddress'
        ) AS object_address,
        COALESCE(
            json_data -> 'rs' -> 'rsDocument' -> 'linearObject' -> 0 ->> 'linearObjectName',
            json_data -> 'rs' -> 'passportObject' ->> 'fullNameObject',
            json_data -> 'rs' -> 'incomingData' -> 'object' -> 'constructionProject' ->> 'constructionProjectName'
        ) AS object_name,
        COALESCE(json_data -> 'rs' -> 'rsDocument' -> 'addressNew' -> 'prefectNew' ->> 'name', json_data -> 'rs' -> 'passportObject' -> 'address' -> 0 -> 'prefect' ->>'name') AS prefect
    FROM rs.documents
    WHERE json_data -> 'rs' -> 'incomingData' -> 'object' ->> 'deloNum' IS NOT NULL
),
distinct_exams AS (
    SELECT DISTINCT
        delo_num_object,
        exam_edit_number
    FROM rs_data_ungrouped
    WHERE exam_edit_number IS NOT NULL AND exam_edit_number != ''
),
rs_data AS (
    SELECT 
        delo_num_object,
        STRING_AGG(
            '<div style="margin-bottom: 5px;">' || exam_edit_number || '</div>', 
            '' ORDER BY exam_edit_number
        ) AS exam_edit_numbers,
        MAX(object_address) AS object_address,
        MAX(object_name) AS object_name,
        MAX(prefect) AS prefect
    FROM (
        SELECT 
            r.delo_num_object,
            r.object_address,
            r.object_name,
            r.prefect,
            d.exam_edit_number
        FROM rs_data_ungrouped r
        LEFT JOIN distinct_exams d ON r.delo_num_object = d.delo_num_object
    ) AS combined_data
    GROUP BY delo_num_object
),
combined_izv_program AS (
    SELECT DISTINCT
        COALESCE(i.delo_num, npc.delo_num) AS delo_num,
        i.reg_date,
        i.reg_number,
        COALESCE(i.object_name, npc.address) AS object_name,
        COALESCE(i.address_landmark, npc.address) AS address,
        npc.program_check_files_links,
        npc.program_check_files_raw,
        npc.objid,
        npc.expected_dates,
        npc.last_expected_date_range
    FROM izv_data i
    FULL OUTER JOIN nadzor_program_combined npc ON i.delo_num = npc.delo_num
    WHERE COALESCE(i.delo_num, npc.delo_num) IS NOT NULL 
      AND i.reg_date IS NOT NULL 
      AND npc.program_check_files_links IS NOT NULL
)
SELECT DISTINCT ON (delo_num, reg_number)
    COALESCE(cip.delo_num, rs.delo_num_object) AS delo_num,
    cip.reg_date,
    cip.reg_number,
    COALESCE(cip.object_name, rs.object_name) AS object_name,
    COALESCE(cip.address, rs.object_address) AS address,
    cip.program_check_files_links AS program_check_files,
    rs.exam_edit_numbers,
    rs.prefect,
    cip.last_expected_date_range AS last_expected_date
FROM combined_izv_program cip
FULL OUTER JOIN rs_data rs ON cip.delo_num = rs.delo_num_object
WHERE COALESCE(cip.delo_num, rs.delo_num_object) IS NOT NULL 
  AND cip.reg_date IS NOT NULL 
ORDER BY delo_num, reg_number, reg_date DESC;
