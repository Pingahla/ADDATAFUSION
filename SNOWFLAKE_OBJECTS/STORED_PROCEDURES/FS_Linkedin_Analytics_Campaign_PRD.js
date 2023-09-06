CREATE OR REPLACE PROCEDURE ADDATAFUSION.DM_ADS.SP_LOAD_LINKEDIN_FS_ADS_ANALYTICS_BY_CAMPAIGN()
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
AS
$$
  var v_campaign_id, v_campaign_dc, v_start_at_dttm, v_end_at_dttm, v_date_range_array, v_pivot_values_array,
  v_pivot_dc, v_pivot_value_dc, v_action_clicks_qty, v_ad_unit_clicks_qty, v_approximate_unique_impressions_qty,
  v_card_clicks_qty, v_card_impressions_qty, v_clicks_qty, v_comments_qty, v_comment_likes_qty, v_company_page_clicks_qty,
  v_conversion_value_in_local_currency_amt, v_cost_in_local_currency_amt, v_cost_in_usd_amt, v_document_completions_qty,
  v_document_first_quartile_completions_qty, v_document_midpoint_completions_qty, v_document_third_quartile_completions_qty,
  v_download_clicks_qty, v_external_website_conversions_qty, v_external_website_post_click_conversions_qty, 
  v_external_website_post_view_conversions_qty, v_follows_qty, v_full_screen_plays_qty, v_impressions_qty, v_job_applications_qty, 
  v_job_apply_clicks_qty, v_landing_page_clicks_qty, v_lead_generation_mail_contact_info_shares_qty, v_lead_generation_mail_interested_clicks_qty,
  v_likes_qty, v_one_click_leads_qty, v_one_click_lead_form_opens_qty, v_opens_qty, v_other_engagements_qty,
  v_post_click_job_applications_qty, v_post_click_job_apply_clicks_qty, v_post_click_registrations_qty, v_post_view_job_applications_qty,
  v_post_view_job_apply_clicks_qty, v_post_view_registrations_qty, v_reactions_qty, v_registrations_qty, v_sends_qty, v_shares_qty,
  v_talent_leads_qty, v_text_url_clicks_qty, v_total_engagements_qty, v_video_completions_qty, v_video_first_quartile_completions_qty, 
  v_video_midpoint_completions_qty, v_video_starts_qty, v_video_third_quartile_completions_qty, v_video_views_qty, v_viral_card_clicks_qty, 
  v_viral_card_impressions_qty, v_viral_clicks_qty, v_viral_comments_qty, v_viral_comment_likes_qty, v_viral_company_page_clicks_qty,
  v_viral_document_completions_qty, v_viral_document_first_quartile_completions_qty, v_viral_document_midpoint_completions_qty,
  v_viral_document_third_quartile_completions_qty, v_viral_download_clicks_qty, v_viral_external_website_conversions_qty,
  v_viral_external_website_post_click_conversions_qty, v_viral_external_website_post_view_conversions_qty, v_viral_follows_qty, 
  v_viral_full_screen_plays_qty, v_viral_impressions_qty, v_viral_job_applications_qty, v_viral_job_apply_clicks_qty, v_viral_landing_page_clicks_qty,
  v_viral_likes_qty, v_viral_one_click_leads_qty, v_viral_one_click_lead_form_opens_qty, v_viral_other_engagements_qty, v_viral_post_click_job_applications_qty,
  v_viral_post_click_job_apply_clicks_qty, v_viral_post_click_registrations_qty, v_viral_post_view_job_applications_qty, v_viral_post_view_job_apply_clicks_qty,
  v_viral_post_view_registrations_qty, v_viral_reactions_qty, v_viral_registrations_qty, v_viral_shares_qty, v_viral_total_engagements_qty, v_viral_video_completions_qty,
  v_viral_video_first_quartile_completions_qty, v_viral_video_midpoint_completions_qty, v_viral_video_starts_qty, v_viral_video_third_quartile_completions_qty, v_viral_video_views_qty;
  var v_version_count;
  var count = 0;

  try {
    var cur_lkdn_campaign = snowflake.execute(
      {sqlText: `SELECT CAMPAIGN_ID, CAMPAIGN_DC, START_AT_DTTM, END_AT_DTTM, DATE_RANGE_ARRAY,
      PIVOT_VALUES_ARRAY, PIVOT_DC, PIVOT_VALUE_DC, ACTION_CLICKS_QTY, AD_UNIT_CLICKS_QTY,
      APPROXIMATE_UNIQUE_IMPRESSIONS_QTY, CARD_CLICKS_QTY, CARD_IMPRESSIONS_QTY, CLICKS_QTY,
      COMMENTS_QTY, COMMENT_LIKES_QTY, COMPANY_PAGE_CLICKS_QTY, CONVERSION_VALUE_IN_LOCAL_CURRENCY_AMT,
      COST_IN_LOCAL_CURRENCY_AMT, COST_IN_USD_AMT, DOCUMENT_COMPLETIONS_QTY, DOCUMENT_FIRST_QUARTILE_COMPLETIONS_QTY,
      DOCUMENT_MIDPOINT_COMPLETIONS_QTY, DOCUMENT_THIRD_QUARTILE_COMPLETIONS_QTY, DOWNLOAD_CLICKS_QTY,
      EXTERNAL_WEBSITE_CONVERSIONS_QTY, EXTERNAL_WEBSITE_POST_CLICK_CONVERSIONS_QTY, 
      EXTERNAL_WEBSITE_POST_VIEW_CONVERSIONS_QTY, FOLLOWS_QTY, FULL_SCREEN_PLAYS_QTY, 
      IMPRESSIONS_QTY, JOB_APPLICATIONS_QTY, JOB_APPLY_CLICKS_QTY, LANDING_PAGE_CLICKS_QTY,
      LEAD_GENERATION_MAIL_CONTACT_INFO_SHARES_QTY, LEAD_GENERATION_MAIL_INTERESTED_CLICKS_QTY,
      LIKES_QTY, ONE_CLICK_LEADS_QTY, ONE_CLICK_LEAD_FORM_OPENS_QTY, OPENS_QTY, OTHER_ENGAGEMENTS_QTY,
      POST_CLICK_JOB_APPLICATIONS_QTY, POST_CLICK_JOB_APPLY_CLICKS_QTY, POST_CLICK_REGISTRATIONS_QTY, 
      POST_VIEW_JOB_APPLICATIONS_QTY, POST_VIEW_JOB_APPLY_CLICKS_QTY, POST_VIEW_REGISTRATIONS_QTY,
      REACTIONS_QTY, REGISTRATIONS_QTY, SENDS_QTY, SHARES_QTY, TALENT_LEADS_QTY, TEXT_URL_CLICKS_QTY,
      TOTAL_ENGAGEMENTS_QTY, VIDEO_COMPLETIONS_QTY, VIDEO_FIRST_QUARTILE_COMPLETIONS_QTY, 
      VIDEO_MIDPOINT_COMPLETIONS_QTY, VIDEO_STARTS_QTY, VIDEO_THIRD_QUARTILE_COMPLETIONS_QTY,
      VIDEO_VIEWS_QTY, VIRAL_CARD_CLICKS_QTY, VIRAL_CARD_IMPRESSIONS_QTY, VIRAL_CLICKS_QTY,
      VIRAL_COMMENTS_QTY, VIRAL_COMMENT_LIKES_QTY, VIRAL_COMPANY_PAGE_CLICKS_QTY,
      VIRAL_DOCUMENT_COMPLETIONS_QTY, VIRAL_DOCUMENT_FIRST_QUARTILE_COMPLETIONS_QTY,
      VIRAL_DOCUMENT_MIDPOINT_COMPLETIONS_QTY, VIRAL_DOCUMENT_THIRD_QUARTILE_COMPLETIONS_QTY,
      VIRAL_DOWNLOAD_CLICKS_QTY, VIRAL_EXTERNAL_WEBSITE_CONVERSIONS_QTY, 
      VIRAL_EXTERNAL_WEBSITE_POST_CLICK_CONVERSIONS_QTY, VIRAL_EXTERNAL_WEBSITE_POST_VIEW_CONVERSIONS_QTY,
      VIRAL_FOLLOWS_QTY, VIRAL_FULL_SCREEN_PLAYS_QTY, VIRAL_IMPRESSIONS_QTY, VIRAL_JOB_APPLICATIONS_QTY,
      VIRAL_JOB_APPLY_CLICKS_QTY, VIRAL_LANDING_PAGE_CLICKS_QTY, VIRAL_LIKES_QTY, VIRAL_ONE_CLICK_LEADS_QTY,
      VIRAL_ONE_CLICK_LEAD_FORM_OPENS_QTY, VIRAL_OTHER_ENGAGEMENTS_QTY, VIRAL_POST_CLICK_JOB_APPLICATIONS_QTY,
      VIRAL_POST_CLICK_JOB_APPLY_CLICKS_QTY, VIRAL_POST_CLICK_REGISTRATIONS_QTY, VIRAL_POST_VIEW_JOB_APPLICATIONS_QTY,
      VIRAL_POST_VIEW_JOB_APPLY_CLICKS_QTY, VIRAL_POST_VIEW_REGISTRATIONS_QTY, VIRAL_REACTIONS_QTY, VIRAL_REGISTRATIONS_QTY,
      VIRAL_SHARES_QTY, VIRAL_TOTAL_ENGAGEMENTS_QTY, VIRAL_VIDEO_COMPLETIONS_QTY, VIRAL_VIDEO_FIRST_QUARTILE_COMPLETIONS_QTY,
      VIRAL_VIDEO_MIDPOINT_COMPLETIONS_QTY, VIRAL_VIDEO_STARTS_QTY, VIRAL_VIDEO_THIRD_QUARTILE_COMPLETIONS_QTY, VIRAL_VIDEO_VIEWS_QTY 
      FROM "ADDATAFUSION"."LINKEDIN"."VW_LINKEDIN_FS_ADS_ANALYTICS_BY_CAMPAIGN"`}
    );

    while (cur_lkdn_campaign.next()) {
          v_campaign_id=cur_lkdn_campaign.getColumnValue(1);
          v_campaign_dc=cur_lkdn_campaign.getColumnValue(2);
          v_start_at_dttm=cur_lkdn_campaign.getColumnValue(3);
          v_end_at_dttm=cur_lkdn_campaign.getColumnValue(4);
          v_date_range_array=cur_lkdn_campaign.getColumnValue(5);
          v_pivot_values_array=cur_lkdn_campaign.getColumnValue(6);
          v_pivot_dc=cur_lkdn_campaign.getColumnValue(7);
          v_pivot_value_dc=cur_lkdn_campaign.getColumnValue(8);
          v_action_clicks_qty=cur_lkdn_campaign.getColumnValue(9);
          v_ad_unit_clicks_qty=cur_lkdn_campaign.getColumnValue(10);
          v_approximate_unique_impressions_qty=cur_lkdn_campaign.getColumnValue(11);
          v_card_clicks_qty=cur_lkdn_campaign.getColumnValue(12);
          v_card_impressions_qty=cur_lkdn_campaign.getColumnValue(13);
          v_clicks_qty=cur_lkdn_campaign.getColumnValue(14);
          v_comments_qty=cur_lkdn_campaign.getColumnValue(15);
          v_comment_likes_qty=cur_lkdn_campaign.getColumnValue(16);
          v_company_page_clicks_qty=cur_lkdn_campaign.getColumnValue(17);
          v_conversion_value_in_local_currency_amt=cur_lkdn_campaign.getColumnValue(18);
          v_cost_in_local_currency_amt=cur_lkdn_campaign.getColumnValue(19);
          v_cost_in_usd_amt=cur_lkdn_campaign.getColumnValue(20);
          v_document_completions_qty=cur_lkdn_campaign.getColumnValue(21);
          v_document_first_quartile_completions_qty=cur_lkdn_campaign.getColumnValue(22);
          v_document_midpoint_completions_qty=cur_lkdn_campaign.getColumnValue(23);
          v_document_third_quartile_completions_qty=cur_lkdn_campaign.getColumnValue(24);
          v_download_clicks_qty=cur_lkdn_campaign.getColumnValue(25);
          v_external_website_conversions_qty=cur_lkdn_campaign.getColumnValue(26);
          v_external_website_post_click_conversions_qty=cur_lkdn_campaign.getColumnValue(27);
          v_external_website_post_view_conversions_qty=cur_lkdn_campaign.getColumnValue(28);
          v_follows_qty=cur_lkdn_campaign.getColumnValue(29);
          v_full_screen_plays_qty=cur_lkdn_campaign.getColumnValue(30);
          v_impressions_qty=cur_lkdn_campaign.getColumnValue(31);
          v_job_applications_qty=cur_lkdn_campaign.getColumnValue(32);
          v_job_apply_clicks_qty=cur_lkdn_campaign.getColumnValue(33);
          v_landing_page_clicks_qty=cur_lkdn_campaign.getColumnValue(34);
          v_lead_generation_mail_contact_info_shares_qty=cur_lkdn_campaign.getColumnValue(35);
          v_lead_generation_mail_interested_clicks_qty=cur_lkdn_campaign.getColumnValue(36);
          v_likes_qty=cur_lkdn_campaign.getColumnValue(37);
          v_one_click_leads_qty=cur_lkdn_campaign.getColumnValue(38);
          v_one_click_lead_form_opens_qty=cur_lkdn_campaign.getColumnValue(39);
          v_opens_qty=cur_lkdn_campaign.getColumnValue(40);
          v_other_engagements_qty=cur_lkdn_campaign.getColumnValue(41);
          v_post_click_job_applications_qty=cur_lkdn_campaign.getColumnValue(42);
          v_post_click_job_apply_clicks_qty=cur_lkdn_campaign.getColumnValue(43);
          v_post_click_registrations_qty=cur_lkdn_campaign.getColumnValue(44);
          v_post_view_job_applications_qty=cur_lkdn_campaign.getColumnValue(45);
          v_post_view_job_apply_clicks_qty=cur_lkdn_campaign.getColumnValue(46);
          v_post_view_registrations_qty=cur_lkdn_campaign.getColumnValue(47);
          v_reactions_qty=cur_lkdn_campaign.getColumnValue(48);
          v_registrations_qty=cur_lkdn_campaign.getColumnValue(49);
          v_sends_qty=cur_lkdn_campaign.getColumnValue(50);
          v_shares_qty=cur_lkdn_campaign.getColumnValue(51);
          v_talent_leads_qty=cur_lkdn_campaign.getColumnValue(52);
          v_text_url_clicks_qty=cur_lkdn_campaign.getColumnValue(53);
          v_total_engagements_qty=cur_lkdn_campaign.getColumnValue(54);
          v_video_completions_qty=cur_lkdn_campaign.getColumnValue(55);
          v_video_first_quartile_completions_qty=cur_lkdn_campaign.getColumnValue(56);
          v_video_midpoint_completions_qty=cur_lkdn_campaign.getColumnValue(57);
          v_video_starts_qty=cur_lkdn_campaign.getColumnValue(58);
          v_video_third_quartile_completions_qty=cur_lkdn_campaign.getColumnValue(59);
          v_video_views_qty=cur_lkdn_campaign.getColumnValue(60);
          v_viral_card_clicks_qty=cur_lkdn_campaign.getColumnValue(61);
          v_viral_card_impressions_qty=cur_lkdn_campaign.getColumnValue(62);
          v_viral_clicks_qty=cur_lkdn_campaign.getColumnValue(63);
          v_viral_comments_qty=cur_lkdn_campaign.getColumnValue(64);
          v_viral_comment_likes_qty=cur_lkdn_campaign.getColumnValue(65);
          v_viral_company_page_clicks_qty=cur_lkdn_campaign.getColumnValue(66);
          v_viral_document_completions_qty=cur_lkdn_campaign.getColumnValue(67);
          v_viral_document_first_quartile_completions_qty=cur_lkdn_campaign.getColumnValue(68);
          v_viral_document_midpoint_completions_qty=cur_lkdn_campaign.getColumnValue(69);
          v_viral_document_third_quartile_completions_qty=cur_lkdn_campaign.getColumnValue(70);
          v_viral_download_clicks_qty=cur_lkdn_campaign.getColumnValue(71);
          v_viral_external_website_conversions_qty=cur_lkdn_campaign.getColumnValue(72);
          v_viral_external_website_post_click_conversions_qty=cur_lkdn_campaign.getColumnValue(73);
          v_viral_external_website_post_view_conversions_qty=cur_lkdn_campaign.getColumnValue(74);
          v_viral_follows_qty=cur_lkdn_campaign.getColumnValue(75);
          v_viral_full_screen_plays_qty=cur_lkdn_campaign.getColumnValue(76);
          v_viral_impressions_qty=cur_lkdn_campaign.getColumnValue(77);
          v_viral_job_applications_qty=cur_lkdn_campaign.getColumnValue(78);
          v_viral_job_apply_clicks_qty=cur_lkdn_campaign.getColumnValue(79);
          v_viral_landing_page_clicks_qty=cur_lkdn_campaign.getColumnValue(80);
          v_viral_likes_qty=cur_lkdn_campaign.getColumnValue(81);
          v_viral_one_click_leads_qty=cur_lkdn_campaign.getColumnValue(82);
          v_viral_one_click_lead_form_opens_qty=cur_lkdn_campaign.getColumnValue(83);
          v_viral_other_engagements_qty=cur_lkdn_campaign.getColumnValue(84);
          v_viral_post_click_job_applications_qty=cur_lkdn_campaign.getColumnValue(85);
          v_viral_post_click_job_apply_clicks_qty=cur_lkdn_campaign.getColumnValue(86);
          v_viral_post_click_registrations_qty=cur_lkdn_campaign.getColumnValue(87);
          v_viral_post_view_job_applications_qty=cur_lkdn_campaign.getColumnValue(88);
          v_viral_post_view_job_apply_clicks_qty=cur_lkdn_campaign.getColumnValue(89);
          v_viral_post_view_registrations_qty=cur_lkdn_campaign.getColumnValue(90);
          v_viral_reactions_qty=cur_lkdn_campaign.getColumnValue(91);
          v_viral_registrations_qty=cur_lkdn_campaign.getColumnValue(92);
          v_viral_shares_qty=cur_lkdn_campaign.getColumnValue(93);
          v_viral_total_engagements_qty=cur_lkdn_campaign.getColumnValue(94);
          v_viral_video_completions_qty=cur_lkdn_campaign.getColumnValue(95);
          v_viral_video_first_quartile_completions_qty=cur_lkdn_campaign.getColumnValue(96);
          v_viral_video_midpoint_completions_qty=cur_lkdn_campaign.getColumnValue(97);
          v_viral_video_starts_qty=cur_lkdn_campaign.getColumnValue(98);
          v_viral_video_third_quartile_completions_qty=cur_lkdn_campaign.getColumnValue(99);
		  v_viral_video_views_qty=cur_lkdn_campaign.getColumnValue(100);


      // Find the previous version of the current record
      var stmt = snowflake.createStatement({
        sqlText: `SELECT COUNT(1) FROM ADDATAFUSION.DM_ADS.FS_LINKEDIN_ADS_ANALYTICS_BY_CAMPAIGN WHERE CAMPAIGN_ID=? AND START_AT_DTTM=? AND END_AT_DTTM=? AND PIVOT_DC=?`,
        binds: [v_campaign_id, v_start_at_dttm, v_end_at_dttm, v_pivot_dc]
      });
      var result = stmt.execute();
      if (result.next()) {
        v_version_count = result.getColumnValue(1);
      }

      if (v_version_count === 0) { // There is no a new record, then we insert a new record
        var stmt = snowflake.createStatement({
          sqlText: `INSERT INTO ADDATAFUSION.DM_ADS.FS_LINKEDIN_ADS_ANALYTICS_BY_CAMPAIGN (CAMPAIGN_ID, CAMPAIGN_DC, START_AT_DTTM, END_AT_DTTM,
          DATE_RANGE_ARRAY, PIVOT_VALUES_ARRAY, PIVOT_DC, PIVOT_VALUE_DC, ACTION_CLICKS_QTY, AD_UNIT_CLICKS_QTY, APPROXIMATE_UNIQUE_IMPRESSIONS_QTY,
          CARD_CLICKS_QTY, CARD_IMPRESSIONS_QTY, CLICKS_QTY, COMMENTS_QTY, COMMENT_LIKES_QTY, COMPANY_PAGE_CLICKS_QTY, CONVERSION_VALUE_IN_LOCAL_CURRENCY_AMT,
          COST_IN_LOCAL_CURRENCY_AMT, COST_IN_USD_AMT, DOCUMENT_COMPLETIONS_QTY, DOCUMENT_FIRST_QUARTILE_COMPLETIONS_QTY, DOCUMENT_MIDPOINT_COMPLETIONS_QTY,
          DOCUMENT_THIRD_QUARTILE_COMPLETIONS_QTY, DOWNLOAD_CLICKS_QTY, EXTERNAL_WEBSITE_CONVERSIONS_QTY, EXTERNAL_WEBSITE_POST_CLICK_CONVERSIONS_QTY,
          EXTERNAL_WEBSITE_POST_VIEW_CONVERSIONS_QTY, FOLLOWS_QTY, FULL_SCREEN_PLAYS_QTY, IMPRESSIONS_QTY, JOB_APPLICATIONS_QTY, JOB_APPLY_CLICKS_QTY,
          LANDING_PAGE_CLICKS_QTY, LEAD_GENERATION_MAIL_CONTACT_INFO_SHARES_QTY, LEAD_GENERATION_MAIL_INTERESTED_CLICKS_QTY, LIKES_QTY, ONE_CLICK_LEADS_QTY,
          ONE_CLICK_LEAD_FORM_OPENS_QTY, OPENS_QTY, OTHER_ENGAGEMENTS_QTY, POST_CLICK_JOB_APPLICATIONS_QTY, POST_CLICK_JOB_APPLY_CLICKS_QTY, POST_CLICK_REGISTRATIONS_QTY,
          POST_VIEW_JOB_APPLICATIONS_QTY, POST_VIEW_JOB_APPLY_CLICKS_QTY, POST_VIEW_REGISTRATIONS_QTY, REACTIONS_QTY, REGISTRATIONS_QTY, SENDS_QTY, SHARES_QTY,
          TALENT_LEADS_QTY, TEXT_URL_CLICKS_QTY, TOTAL_ENGAGEMENTS_QTY, VIDEO_COMPLETIONS_QTY, VIDEO_FIRST_QUARTILE_COMPLETIONS_QTY, VIDEO_MIDPOINT_COMPLETIONS_QTY,
          VIDEO_STARTS_QTY, VIDEO_THIRD_QUARTILE_COMPLETIONS_QTY, VIDEO_VIEWS_QTY, VIRAL_CARD_CLICKS_QTY, VIRAL_CARD_IMPRESSIONS_QTY, VIRAL_CLICKS_QTY, VIRAL_COMMENTS_QTY,
          VIRAL_COMMENT_LIKES_QTY, VIRAL_COMPANY_PAGE_CLICKS_QTY, VIRAL_DOCUMENT_COMPLETIONS_QTY, VIRAL_DOCUMENT_FIRST_QUARTILE_COMPLETIONS_QTY, VIRAL_DOCUMENT_MIDPOINT_COMPLETIONS_QTY,
          VIRAL_DOCUMENT_THIRD_QUARTILE_COMPLETIONS_QTY, VIRAL_DOWNLOAD_CLICKS_QTY, VIRAL_EXTERNAL_WEBSITE_CONVERSIONS_QTY, VIRAL_EXTERNAL_WEBSITE_POST_CLICK_CONVERSIONS_QTY,
          VIRAL_EXTERNAL_WEBSITE_POST_VIEW_CONVERSIONS_QTY, VIRAL_FOLLOWS_QTY, VIRAL_FULL_SCREEN_PLAYS_QTY, VIRAL_IMPRESSIONS_QTY, VIRAL_JOB_APPLICATIONS_QTY,
          VIRAL_JOB_APPLY_CLICKS_QTY, VIRAL_LANDING_PAGE_CLICKS_QTY, VIRAL_LIKES_QTY, VIRAL_ONE_CLICK_LEADS_QTY, VIRAL_ONE_CLICK_LEAD_FORM_OPENS_QTY, VIRAL_OTHER_ENGAGEMENTS_QTY,
          VIRAL_POST_CLICK_JOB_APPLICATIONS_QTY, VIRAL_POST_CLICK_JOB_APPLY_CLICKS_QTY, VIRAL_POST_CLICK_REGISTRATIONS_QTY, VIRAL_POST_VIEW_JOB_APPLICATIONS_QTY,
          VIRAL_POST_VIEW_JOB_APPLY_CLICKS_QTY, VIRAL_POST_VIEW_REGISTRATIONS_QTY, VIRAL_REACTIONS_QTY, VIRAL_REGISTRATIONS_QTY, VIRAL_SHARES_QTY, VIRAL_TOTAL_ENGAGEMENTS_QTY,
          VIRAL_VIDEO_COMPLETIONS_QTY, VIRAL_VIDEO_FIRST_QUARTILE_COMPLETIONS_QTY, VIRAL_VIDEO_MIDPOINT_COMPLETIONS_QTY, VIRAL_VIDEO_STARTS_QTY,
          VIRAL_VIDEO_THIRD_QUARTILE_COMPLETIONS_QTY, VIRAL_VIDEO_VIEWS_QTY) 
		  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?,
          ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
          binds: [v_campaign_id, v_campaign_dc, v_start_at_dttm, v_end_at_dttm, v_date_range_array, v_pivot_values_array,
                  v_pivot_dc, v_pivot_value_dc, v_action_clicks_qty, v_ad_unit_clicks_qty, v_approximate_unique_impressions_qty,
                  v_card_clicks_qty, v_card_impressions_qty, v_clicks_qty, v_comments_qty, v_comment_likes_qty, v_company_page_clicks_qty,
                  v_conversion_value_in_local_currency_amt, v_cost_in_local_currency_amt, v_cost_in_usd_amt, v_document_completions_qty,
                  v_document_first_quartile_completions_qty, v_document_midpoint_completions_qty, v_document_third_quartile_completions_qty,
                  v_download_clicks_qty, v_external_website_conversions_qty, v_external_website_post_click_conversions_qty, 
                  v_external_website_post_view_conversions_qty, v_follows_qty, v_full_screen_plays_qty, v_impressions_qty, v_job_applications_qty, 
                  v_job_apply_clicks_qty, v_landing_page_clicks_qty, v_lead_generation_mail_contact_info_shares_qty, v_lead_generation_mail_interested_clicks_qty,
                  v_likes_qty, v_one_click_leads_qty, v_one_click_lead_form_opens_qty, v_opens_qty, v_other_engagements_qty,
                  v_post_click_job_applications_qty, v_post_click_job_apply_clicks_qty, v_post_click_registrations_qty, v_post_view_job_applications_qty,
                  v_post_view_job_apply_clicks_qty, v_post_view_registrations_qty, v_reactions_qty, v_registrations_qty, v_sends_qty, v_shares_qty,
                  v_talent_leads_qty, v_text_url_clicks_qty, v_total_engagements_qty, v_video_completions_qty, v_video_first_quartile_completions_qty, 
                  v_video_midpoint_completions_qty, v_video_starts_qty, v_video_third_quartile_completions_qty, v_video_views_qty, v_viral_card_clicks_qty, 
                  v_viral_card_impressions_qty, v_viral_clicks_qty, v_viral_comments_qty, v_viral_comment_likes_qty, v_viral_company_page_clicks_qty,
                  v_viral_document_completions_qty, v_viral_document_first_quartile_completions_qty, v_viral_document_midpoint_completions_qty,
                  v_viral_document_third_quartile_completions_qty, v_viral_download_clicks_qty, v_viral_external_website_conversions_qty,
                  v_viral_external_website_post_click_conversions_qty, v_viral_external_website_post_view_conversions_qty, v_viral_follows_qty, 
                  v_viral_full_screen_plays_qty, v_viral_impressions_qty, v_viral_job_applications_qty, v_viral_job_apply_clicks_qty, v_viral_landing_page_clicks_qty,
                  v_viral_likes_qty, v_viral_one_click_leads_qty, v_viral_one_click_lead_form_opens_qty, v_viral_other_engagements_qty, v_viral_post_click_job_applications_qty,
                  v_viral_post_click_job_apply_clicks_qty, v_viral_post_click_registrations_qty, v_viral_post_view_job_applications_qty, v_viral_post_view_job_apply_clicks_qty,
                  v_viral_post_view_registrations_qty, v_viral_reactions_qty, v_viral_registrations_qty, v_viral_shares_qty, v_viral_total_engagements_qty, v_viral_video_completions_qty,
                  v_viral_video_first_quartile_completions_qty, v_viral_video_midpoint_completions_qty, v_viral_video_starts_qty, v_viral_video_third_quartile_completions_qty, v_viral_video_views_qty]
        });
        stmt.execute();
        count++;
      } 
    }

    return "Procedure SP_LOAD_LINKEDIN_FS_ADS_ANALYTICS_BY_CAMPAIGN completed successfully, records added: " + count;
  } catch (err) {
    return "Error executing procedure: " + err.message;
  }
$$;