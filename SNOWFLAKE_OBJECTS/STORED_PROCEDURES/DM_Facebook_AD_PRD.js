CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_CD2_AD()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_ad_id, v_ad_nm, v_bid_amt, v_bid_type_dc, v_created_dttm, v_status_dc;
  var v_row_dc;

  try {
    var cur_ad = snowflake.execute(
      {sqlText: `SELECT ROW_CHANGE_DC, AD_ID, AD_NM, BID_AMT, BID_TYPE_DC, CREATED_DTTM, STATUS_DC FROM ADDATAFUSION.FACEBOOK_ADS.VW_CHANGES_TO_CD2_AD`}
    );

    while (cur_ad.next()) {
      v_row_dc = cur_ad.getColumnValue(1);
      v_ad_id = cur_ad.getColumnValue(2);
      v_ad_nm = cur_ad.getColumnValue(3);
      v_bid_amt = cur_ad.getColumnValue(4);
      v_bid_type_dc = cur_ad.getColumnValue(5);
      v_created_dttm = cur_ad.getColumnValue(6);
      v_status_dc = cur_ad.getColumnValue(7);
      

      if (v_row_dc === "UPDATE") { //there is a previous record and we need update it and insert the new one
        var stmt = snowflake.createStatement({
          sqlText: `UPDATE ADDATAFUSION.DM_ADS.CD2_AD SET EFF_TO_DTTM = CURRENT_TIMESTAMP(), CURR_REC_IND = 'N' WHERE AD_ID = :1 AND CURR_REC_IND = 'Y'`,
          binds: [v_ad_id]
        });
        stmt.execute();
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.CD2_AD (AD_ID, AD_NM, BID_AMT, BID_TYPE_DC, CREATED_DTTM, STATUS_DC, CURR_REC_IND, EFF_FM_DTTM, EFF_TO_DTTM) VALUES (:1, :2, :3, :4, :5, :6, 'Y', CURRENT_TIMESTAMP(), '9999-12-31 23:59:00.000')`,
          binds: [v_ad_id, v_ad_nm, v_bid_amt, v_bid_type_dc, v_created_dttm, v_status_dc]
        });
        stmt.execute();
      }

      else if (v_row_dc === "APPEND") { // There is no previous record, we insert a new record
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.CD2_AD (AD_ID, AD_NM, BID_AMT, BID_TYPE_DC, CREATED_DTTM, STATUS_DC, CURR_REC_IND, EFF_FM_DTTM, EFF_TO_DTTM) VALUES (:1, :2, :3, :4, :5, :6, 'Y', CURRENT_TIMESTAMP(), '9999-12-31 23:59:00.000')`,
          binds: [v_ad_id, v_ad_nm, v_bid_amt, v_bid_type_dc, v_created_dttm, v_status_dc]
        });
        stmt.execute();
      }  
      else if (v_row_dc === "FINAL") { // The record was removed from de Landing zone, and we need to deactivate it
        var stmt = snowflake.createStatement({
          sqlText: `UPDATE ADDATAFUSION.DM_ADS.CD2_AD SET EFF_TO_DTTM = CURRENT_TIMESTAMP(), CURR_REC_IND = 'N' WHERE AD_ID = :1 AND CURR_REC_IND = 'Y'`,
          binds: [v_ad_id]
        });
        stmt.execute();
      }
    }

    return "Procedure completed successfully.";
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;