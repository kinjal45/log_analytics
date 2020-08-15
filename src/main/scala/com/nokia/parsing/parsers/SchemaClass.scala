package com.nokia.parsing.parsers

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object SchemaClass  {
  /** *****************************************
    * Capture variables with schema for log file
    * ******************************************
    */
  val flexi_bsc__boot_info__zwdi_bootimage = List("line_number","FUNCTIONAL_UNIT", "PLUG_IN_UNIT_TYPE_INDEX", "BOOT_PACKAGE_FLASH_MEMORY", "BOOT_PACKAGE_DISK_FILE")
  val flexi_bsc__bts_sw_lmu_and_abis_pool_data__zewl_bts_sw_pack = List("line_number", "BUILD_ID", "TYPE", "REL_VER", "INITIAL", "MASTER_FILE", "SUBDIR", "CONNECTED_SITES")
  val flexi_bsc__bsc_alarm_parameters__zabo_bsc_blk_alarm = List("line_number", "AlarmNo", "AlarmText", "Units")
  val `flexi_bsc__switch info__training_switch_info` = List("line_number", "Unit", "Phy_State", "Location", "Info")
  val flexi_bsc__unit_information__zudh_diagnosis_history = List("line_number","BSC", "Unit", "Param1", "Param2", "Message")
  val flexi_bsc__bsc_alarm__zahp_zaho_bsc_alarms = List("line_number", "BSC", "Unit", "Alarm_Type", "DTTM", "Severity", "Not_type",
    "Param1", "Issuer", "Trans_id", "Alarm_id", "Alarm_text", "Supp1", "Supp2", "Supp3", "Supp4", "Supp5", "Supp6", "Supp7", "Supp8", "Supp9")
  val flexi_bsc__bts_alarm_history__zeoh_bts__alarm_history = List("line_number", "Type", "BSC_Name", "BCF_ID", "BTS_ID", "Tag", "DTTM",
    "Sev", "Alarm_or_Cancel", "TRX_ID", "BTS_NAME", "Param", "Notification_ID", "Alarm_No", "Alarm_text", "Supp_Info")
  val flexi_bsc__system_configuration__zwoi_pr_file = List("line_number","PARAMETER_CLASS", "PARAMETER_NAME", "IDENTIFIER", "NAME_OF_PARAMETER", "VALUE", "CHANGE_POSSIBILITY")
  val flexi_bsc__sw_configuration__zwqo_run_bscpack, flexi_bsc__sw_configuration__fb_unit_info_zusi = List("line_number","WORKING_PACKAGES_IN_UNITS_PAGE", "MBA", "UNIT", "NAME", "STATUS", "PACKAGE_ID", "REP_ID")
  val flexi_bsc__system_configuration__zwos_pr_file = List("line_number","PARAMETER_CLASS", "PARAMETER_NAME", "IDENTIFIER", "NAME_OF_PARAMETER", "ACTIVATION_STATUS")
  val flexi_bsc__bsc_alarm_history_txt__bsc_alarm, mcbsc__bsc_alarm_history__bsc_alarm = List("line_number","BSC", "UNIT", "ALARM_TYPE","DTTM","SEVERITY", "NOT_TYPE", "PARAM1", "PARAM2", "TRANS_ID", "ALARM_ID", "ALARM_TEXT", "ALARM_SUP")
  val flexi_bsc__system_configuration__zwti_c = List("line_number","CART", "HALL", "LOC", "P1", "P2", "P3", "P4", "AG", "AL")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data__zego_bsc_timers = List("line_number", "PARAMETER", "VALUE", "PRESET_VALUE", "MODIFIABLE")
  val flexi_bsc__oem_network__zqdi_max_osi_data = List("line_number","AE_NAME", "APPL", "NETADDR", "STATE", "UNIT", "FAMID", "PROCID", "AP_TYPE", "AP_TITLE", "AEQ", "P-SELECTOR", "S-SELECTOR", "T-SELECTOR")
  val flexi_bsc__io_system__zw7i_fea_state = List("line_number", "FEATURE_CODE", "FEATURE_NAME", "FEATURE_STATE", "FEATURE_CAPACITY")
  val flexi_bsc__database_status__zdbs_dbstatecheck = List("line_number","BSC_NAME","DTTM", "WO_DATABASE", "WO_OCCURANCE", "WO", "WO_STATE", "WO_SUBSTATE", "SP_DATABASE",
    "SP_OCCURANCE", "SP", "SP_STATE", "SP_SUBSTATE", "MEMORY_UPDATES_PREVENTED", "DISK_UPDATES_PREV", "TRANSACTION_OVER_FLOW", "BACKUP_IS_ON")
  val flexi_bsc__io_system__zi2h_protocol_state = List("line_number", "IPv4_SSH_ENABLED", "IPv4_SSH_PORT", "IPv6_SSH_ENABLED", "IPv6_SSH_PORT", "PRIVATE_RSA_KEY_NAME", "PRIVATE_DSA_KEY_NAME",
    "LOGIN_GRACE_TIME", "SFTP_ENABLED", "IPv4_FTP_ENABLED", "IPv4_FTP_PORT", "IPv6_FTP_ENABLED", "IPv6_FTP_PORT")
  val flexi_bsc__cell_broadcast_status__zecp_cell_broadcast_msg = List("line_number","MSG_INDEX", "MSG_ID", "MSG_CODE", "MSG_INFO", "REP_RATE", "GEP_SCOPE", "CODING_GROUP",
    "MC", "LANG_ALPH")
  val flexi_bsc__utpfil_and_memory_files__fb_ipc_obsolete_utpfil = List("line_number","MCMU", "FAMILY", "UTPFIL", "UTPFILval1", "UTPFILval2")
  val flexi_bsc__bsc_alarm_history__zahp_bsc_alarm_histroy = List("history", "supp",
    "alarm_txt", "alarm_no", "notification_id", "cart", "issuer", "type", "param", "date_time", "alart_type", "unit", "bsc","line_number")
  val flexi_bsc__bsc_alarm_history_txt__computer_logs = List("caller", "write_time", "parameters",
    "user_text", "user_date", "user_date1", "user_date2","line_number")
  val flexi_bsc__bsc_alarms__zaho_bsc_alarm_check = List("history", "supp", "alarm_txt", "alarm_no", "notification_id", "cart", "issuer",
    "type", "param", "date_time", "alart_type", "unit", "bsc","line_number")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeek_fbpp_status = List("rnw_plan_database_state", "rnw_configuration_id",
    "rnw_plan_configuration_id", "rnw_fallback_configuration_id","line_number")
  val flexi_bsc__measurement_status__zifi_event_sending_status = List("event_sending","line_number")
  val flexi_bsc__finaletpaping__ip_ping_results = List("ip", "packets_transmitted", "received", "packet_loss", "time","line_number")
  val flexi_bsc__trx_deletion_error__zerd = List("error", "error_desc","line_number")
  //val flexi_bsc__bsc_alarm_history__bsc_alarm = List("alarm_supp", "alarm_txt", "alarm_no", "trans_id", "param2", "param1", "not_type", "severity", "date_time", "alart_type", "unit", "bsc")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_c_m_plane_ip = List("line_number","bts_cu_plane_ip_address", "bts_m_plane_ip_address","csubnet",
    "msubnet")
  val flexi_bsc__system_configuration__zwti_u_configuration = List("unit", "param1", "param2", "location", "master","line_number")
  val mcbsc__bsc_alarms__zaho_bsc_alarm_check = List("history", "supp", "alarm_txt", "alarm_no", "notification_id", "cart", "issuer", "type", "param",
    "date_time", "alart_type", "unit", "bsc","line_number")
  val flexi_bsc__ip_configurations__zqri_bcsu_internal_ip,flexi_bsc__ip_configurations__fb_ipc_zqri_bcsu = List("bcsu","vlan1_ip_subnet","vlan1_attr","vlan1_mtu","vlan1_state","vlan1","vlan2_ip_subnet","vlan2_attr","vlan2_mtu","vlan2_state","vlan2",
    "vlan3_ip_subnet","vlan3_attr","vlan3_mtu","vlan3_state","vlan3","vlan4_ip_subnet","vlan4_attr","vlan4_mtu","vlan4_state","vlan4",
    "vlan5_ip_subnet","vlan5_attr","vlan5_mtu","vlan5_state","vlan5","line_number")
  val mcrnc__rnchw__rnchw_smartctl_hdd_grown_defect = List("box", "smart_health_status", "elements_in_grown_defect_list","line_number")
  val mcrnc__rncsignaling__rncsignaling_sccp_destination_point_code = List("line_number", "Point_code_Name", "Point_code_Id", "Remote_AS_Name", "Point_code",
    "SAP_Profile_Name", "PC_Type", "Status", "Include_PC_in_called_party_address", "SST_on_DPC_Accessible", "SCCP_Timer_Profile_Name")
  val mcrnc__rncsignaling__rncsignaling_ss7_association = List("line_number", "association_id", "primary_local_ip_addr", "secondary_local_ip_addr",
    "local_client_port", "local_as_name", "vrf_name", "node", "remote_as_name", "primary_remote_ip_add", "secondary_remote_ip_addr",
    "remote_port", "exchange_mode", "sctp_profile", "communication_type", "admin_state", "role", "status", "active_path", "destination_reachable", "destination_reachable2")
  val mcrnc__rncsignaling__rncsignalingsccpsubsystemremote = List("line_number", "SCCP_SubSystem_Identifier", "SCCP_SubSystem",
    "SCCP_SubSystem_Number", "SAP_Profile_Name", "Dest_Point_code_Name", "Concerned_SubSystem", "Status")
  val wcdma_rnc__omu_computer_logs__omu_computer_logs: List[String] = List("line_number", "CALLER", "WRITE_TIME", "PARAMETERS", "USER_TEXT", "USER_DATA","USER_DATA1","USER_DATA2")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeeo_bsc_dfca = List("line_number", "EXPECTED_BSC_BSC_INTERFACE_DELAY")
  val flexi_mr_bts_lte__rawalarmhistory__mrbts_rawalarmhistory = List("line_number", "Param", "DTTM", "FaultId", "Fault_Description", "FaultSrc", "SerialNumber")
  val flexi_bsc__computer_logs__zdde_computer_logs = List("line_number","UNIT", "CALLER", "WRITE_TIME", "PARAMETERS", "USER_TEXT", "USER_DATA")
 // val flexi_bsc__ip_configurations__zqri_ip_configuration = List("line_number", "BSC", "DTTM", "Unit", "UNIT_PIU", "VLAN_ID", "Interface", "Interface1", "Attribute", "IP_ADD", "STATE", "MTU", "Attr")
   val flexi_bsc__ip_configurations__zqri_ip_configuration = List("line_number", "BSC", "DTTM", "Unit", "UNIT_PIU", "VLAN_ID", "Interface", "Interface1", "Attribute", "IP_ADD", "STATE", "MTU")
   val mcrnc__rncrnw__radio_network_iuparams = List("line_number","INTERFACE", "IUId", "CNDomainVersion", "SignPointCode", "IuState", "IuLinkState",
    "IPBasedRouteId", "IPQMid", "DestIPAddress", "IPNetmask")
  val mcrnc__rnchw__embedded_sw_status = List("line_number","CHASSIS", "FIRMWARE", "ACTIVE_MAINBANK", "ROLLBACK_BACKUPBANK", "PENDINGBANK")
  val  mcrnc__rnchw__embedded_sw_version=List("line_number","FIRMWARE","ACTIVE_MAINBANK","ROLLBACK_BACKUPBANK","PENDINGBANK","BASELINE_BANK")
  val mcrnc__rnchw__lmp_hw_status = List("line_number","RNCid","LMP_ID", "NODE_STATUS")
  val mcrnc__rnchw__rnchwsfpstatus = List("line_number","LMPParam", "SFP_NAME", "ENAB_DISAB", "LEFTPARAM", "RIGHT1", "RIGHT2", "RIGHT3")
  val flexi_mr_bts_lte__1011_rawalarmhistory__parse_rawalarmhistory = List("date","time","line_number","Faults", "FAULT_ID", "FAULT_NAME", "SOURCE", "SOURCE_ADDITIONAL")
  val flexi_bsc__swu__swu_monitor_esb24 = List("line_number", "MIRROR", "PARAM1")
  val flexi_bsc__ip_configurations__zqri_etpsig_m = List("line_number", "MCMU", "VLAN", "STATE", "MTU", "ATTR", "IP_ADDR")
  //val flexi_bsc__unit_information__zusi_unitinfo = List("BSC", "DTTM", "UNIT", "PHYS", "STATE", "LOCATION", "INFO")
  val flexi_bsc__bsc_alarms__zahp_zaho_bsc_alarms = List("line_number", "BSC", "UNIT", "ALARM_TYPE", "DTTM", "SEVERITY", "NOT_TYPE", "PARAM1",
    "ISSUER", "TRANS_ID", "ALARM_NO", "ALARM_TXT", "SUPP")
  val flexi_bsc__etp_logs__etp_active_call = List("line_number", "NUM_CS_CALLS_ACTIVE", "ETP_ID")
  val flexi_bsc__bts_alarms__zeol_bts_alarms = List("line_number", "BSC_NAME", "BCF_ID", "BTS_ID", "TAG", "DTTM", "SEVERITY", "ALARM_OR_CANCEL", "TRX_ID",
    "BTS_NAME", "NOTIFICATION_ID", "ALARM_NO", "ALARM_TXT", "SUPP_INFO")
  val flexi_bsc__sw_configuration__zwqo_cr_omupack = List("line_number", "SW_PACKAGE", "STATUS",
    "ENV", "DIRECTORY", "DEF", "ACT")
  val flexi_bsc__locked_files__ziwx_alhistgx = List("line_number", "ALHISTGX-FILE_VERSION",
    "ALHISTGX-FILE_NO", "ALHISTGX-FILE_LENGTH")
  val flexi_bsc__io_system__zw7i_fea_usage = List("feature_code", "feature_usage","line_number")
  val flexi_bsc__switch_info__zw6g_topology = List("topology", "bsc_type","line_number")
  val flexi_bsc__clock_and_lapd_status__zdti_lapd_check = List("name", "num", "unit",
    "interface_side", "pcm_tsl_tsl", "sapi", "working_state","line_number")
  val flexi_bsc__clock_and_lapd_status__zdri_sync_input = List("unit", "state", "used_input",
    "priority","line_number")
  val flexi_bsc__ip_configurations__zqvi_pad_parameters = List("nr", "padp_name", "current_value","line_number")
  val flexi_bsc__ss7_network_txt__znci_m3ua_based_signalling_links = List("m3ua_link", "m3ua_link_set",
    "association_set", "m3ua_param_set","line_number")
  val flexi_bsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_m_ = List("unit", "pool",
    "pool_percentage", "common_buffers", "common_buffers_percentage", "family_environment",
    "family_environment_percentage", "private_buffers", "private_buffers_percentage",
    "message_buffers", "message_buffers_percentage", "free_memory", "free_memory_percentage",
    "free_headers_count", "free_headers_count_percentage","line_number")
  val flexi_bsc__bsc_alarm_history_txt__zahp_alarm_sup = List("bsc", "supp", "alarm_txt", "alarm_id",
    "trans_id", "issuer", "param1", "not_type", "severity", "date_time", "unit", "alarm_type","line_number")
  val flexi_bsc__boot_info_txt__zwdi = List("line_number", "UNIT", "PIU_TYPE", "BOOT_PKG_FLASH", "FLASH_VERSION", "DATE", "RANDOM",
    "BOOT_PKG_DISK", "DISK_VERSION", "DV_DATE", "RANDOM1")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeeo_dinho = List("line_number", "DINHO")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__fb_bsc_bcf_mc_sbnt = List("line_number", "BCFDATA", "CUPLANE_IP1", "CUPLANE_IP2",
    "CUPLANE_IP3", "CUPLANE_IP4", "CUPLANE_SubnetMask", "MPLANE_IP1", "MPLANE_IP2", "MPLANE_IP3", "MPLANE_IP4", "MPLANE_SubnetMask")
  val flexi_bsc__supplementary_ss7_network__zosk = List("mtpsccp", "sp_code_hd", "sp_name", "reporting_status","line_number")
  val flexi_bsc__boot_info_txt__zwdi_boot_image_of_units = List("line_number", "Functional_Unit", "Plug_In_Unit_Type_Index", "Boot_Package", "Flash_Memory",
    "Disk_File")
  val flexi_bsc__ip_configurations__zqkb_static_routes = List("line_number", "UNIT", "IPROUTE", "PHY",
    "NUM", "DF")
  val flexi_bsc__ip_configurations__zqkb_statroute = List("line_number", "UNIT", "IP1", "IP2", "IP3", "IP4", "DF", "DF_IP1", "DF_IP2", "DF_IP3", "DF_IP4")
  val flexi_bsc__supervision_and_disk_status_txt__fb_unit_memory_check = List("unit", "pool", "free_memory","line_number")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeoe_bts_blocked_alarms = List("param1", "param2", "unit",
    "date_time","line_number")
  val flexi_bsc__unit_information__fb_mc_unit_status = List("unit", "info","line_number")
  val flexi_bsc__bts_sw_lmu_and_abis_pool_data__zewo_bcf_swpack = List("bcf_number", "nw_status",
    "nw_build_id", "nw_version", "nw_subdir", "nw_state", "nw_swmaster", "bu_status", "bu_build_id",
    "bu_version", "bu_subdir", "bu_state", "bu_swmaster", "fb_status", "fb_build_id",
    "fb_version", "fb_subdir", "fb_state", "fb_swmaster","line_number")
  val flexi_bsc__bts_seg_and_trx_parameters__zeqo_basic1 = List("line_number","BCFNAME", "BTSNAME", "BSCNAME", "PARAMNAME", "SHORTNAME", "PARAMVALUE")
  val flexi_bsc__ss7_network__znci_signalling_link_data = List("line_number", "LINK", "LINK_SET", "PCM-TSL", "UNIT", "TERM", "TF", "LOG_UNIT", "LOG_TERM", "PAR_SET",
    "BIT_RATE", "MTP2_REQ")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_bcf_all_parameters1 = List("bcf_number","param_name","param_value","line_number")
  val flexi_bsc__bts_seg_and_trx_parameters__zeqo_btsparameter = List("bcf","bts","bts_adm_state",
    "bts_state","gena","psei","pcu_index","pcu_id","pcu_state","bvci","nsei1","bvc1_state","nsei2",
    "bvc2_state","nsei3","bvc3_state","line_number")
  val flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__zewl_output = List("bsc_name","date_time","build_id","type",
    "rel_ver","initial","mf_name","subdir","conn","line_number")
  val flexi_bsc__preprocessor_sw__zdpp_cls_preproinput = List("bsc","date_time","cls","cls1","rom_sw","fpga_revision","cpld_usercode","line_number")
  val sran_bts__extendedsysteminfo__extendedsysteminfo_diskusage = List("file_system","1k_blocks","used","available","use_percentage","mount_on","line_number")
  val flexi_bsc__etxx_configuration__zwti_pp_etphotlink=List("line_number","UNIT","PIU","PORT","UNIT_1","PIU_1","PORT_1")
  val flexi_bsc__etxx_configuration__zesl_pcu_pcuonetp=List("line_number","ETPx","ETP_INDEX","BCSU","PCU")
  val flexi_bsc__supplementary_ss7_network__zoyo=List("line_number","ParameterSetName","SET","RTOMIN","RTOMAX","RTOINIT","HBInterval","SackPeriod","PathMaxRET",
    "ASSMAXRET","CHECKSUM","BUNDLING")
  val flexi_bsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_p=List("line_number","UNIT","TIME_USAGE_ALLOWED","LOAD_ALLOWED","LOAD_PERCENT","CLASS_FOR_CRRQ",
    "CLOCK_FREQUENCY_MHZ")
  
  val sran_bts__scf__sbtstimezone = List("offset","country","line_number")
  val sran_bts__scf__scf_pmax = List("mrbts_id","lnbts_id","lncel_id","line_number")
  val sran_bts__dump_routing_table__routingtablenew = List("param1","param2","param3","param4","param5","param6","param7","param8","gateway_ip","rmtu","pmtu",
    "otf","used_flag","line_number")
  val sran_bts__snapshot_file_list__snapshot_file_list_countofmodules = List("param","line_number")
  val sran_bts__dump_routing_table__routingtable = List("src_ip","dst_ip","gateway_ip","rmtu","pmtu","otf","used_flag","proto","src_port_min","src_port_max","dst_port_min","dst_port_max","line_number")
  val sran_bts__1011_rawalarmhistory__bts_1011_rawalarmhistory = List("type","date","time","fault_id","fault_name","source","source_additional","line_number")
  val flexi_bsc__swu_7__tsn_0911_swu_7_edgeport = List("low_port","high_port","line_number")
  val flexi_bsc__switch_info__switch_working_state = List("unit","phy_state","location","info","line_number")
  val flexi_bsc__ztpi_meas_state__measurement_status = List("BSC","DTTM","TYPE","LAST_MODIFIED_DATE","LAST_MODIFIED_TIME","ADMIN_STATE","OPR_STATE")
  val flexi_bsc___ip_configurations=List("line_number", "unit","Txpackets" ,"RxPackets","BroadcastTX" ,"BroadcastRX" ,"VLANtx","VLANrx","Errors" ,"TXcarrierSenseLost" )
  val flexi_bsc__ss7_network__zobl_ip_broadcast = List("line_number", "CONCERNED_LOCAL_SUBSYSTEMS","BROADCAST_GROUPS")
  /* val mcrnc__rnchw__embedded_sw_version = List("line_number", "ALARM_ID","SPECIFIC_PROBLEM","MANAGED_OBJECT","SEVERITY","CLEARED",
     "CLEARING","ACKNOWLEDGED","ACK_USER_ID","ACK_TIME","ALARM_TIME","EVENT_TYPE","APPLICATION",
     "IDENTIF_APPL_ADDL_INFO","APPL_ADDL_INFO")
  */
  val sran_bts__scf__x2_link_status =List("line_number", "LNADJ","LINK_STATUS")
  val sran_bts__snapshot_file_list__dsp_crash =List("line_number", "PARAM1","PARAM2")
  val sran_bts__011_rawalarmhistory__rawalarmhistory =List("line_number", "DTTM","TYPE","FAULT_DTTM","FAULT_ID","FAULT_NAME","SOURCE","SOURCE_ADDITIONAL")
  val sran_bts__scf__scf_dlmimotype =List("line_number", "MIMO_TYPE","MRBTS_ID","LNBTS_ID","LNCEL_ID")
  val sran_bts__runtime__rfmodulefaults=List("line_number","FaultName","FaultId","Source","Severity","State","FaultCause")
  val mcrnc__rncalarm__mcrnc_active_alarm=List("line_number","Alarm_ID","Specific_Problem","Managed_Object","Severity","Cleared","Clearing","Acknowledged","Ack_user_ID",
    "Ack_time","Alarm_time","Event_type","Application","Identif_appl_addl_info","Appl_addl_info")
  val sran_bts__lrm_dump__lrm_dump=List("line_number","LCG_ID","CaGroup_ID","HspaConfig","MulticastPSId","MulticastSMId","MulticastEVAMId","StartAllDone",
    "HSUPA_Res_Allocation_Blocked","MinNbrOfHsRachCfs",
    "ChannelCapacityExceeded_DCH_UL","ChannelCapacityExceeded_DCH_DL","ChannelCapacityExceeded_HSUPA_SM_1","ChannelCapacityExceeded_HSUPA_SM_2",
    "ChannelCapacityExceeded_HSDPA")
  val sran_bts__scf__scf_channelgrouping=List("line_number","CELLMAPPING","LCEL","CHANNELGROUP","CHANNEL","DIRECTION","MRBTS","EQM","APEQM","RMOD","ANTL")
  val sran_bts__extendedsystem__cp_traffic_wcdma = List("packets","bytes","target","protocol","opt","in","out","source","destination","additional_info","line_number")
  val wcdma_rnc__functional_units__zusi_full = List("unit","phys","log","state","info","line_number")
  val wcdma_rnc__diagnostics__diagnostics_report_history_unit_diagnose_status = List("unit","execution_status","line_number")
  val flexi_mr_bts_lte__trs__parse_ethlk = List("ethlk","param","value","line_number")
  val wcdma_rnc__active_alarms__rnc_active_alarm = List("alarm_consecutive_number","rnc_id",
    "dumm1","date_time","severity","notification_type","unit","wcel","alarm_number","alarm_description","set_by","set_at","suppl_info1","suppl_info2","line_number")
  val flexi_mr_bts_lte__messages_2__read_messages_2 = List("date","na","error_type","message","line_number")
  val flexi_mr_bts_lte__runtime_btsom_log__1011_runtime = List("runtime_btsom","line_number")
  val flexi_mr_bts_lte__011_rawalarmhistory__011_rawalarmhistory_alldata = List("fault_status","date_time","fault_id","fault_name","source","fault_rised","line_number")
  val sran_bts__tas_extended_startup__tas_extended_startup_lmpipcheck=List("line_number", "PARAM")
  val mcbsc__mcbc___zw6t_flowcontrol=List("line_number", "FLOWCONTROL")

  val flexi_bsc__ip__fb_ipc_zqri_etpxunit = List("line_number", "ETPxUNIT","PARAM1","PARAM2","IP_ADDR","SUBNET")
  val sran_bts__scf__scf_parameters_all = List("line_number", "param","value")
  val flexi_mr_bts_lte__scf_xml__fetch_lnrel = List("line_number", "LNREL","PARAM","VALUE")
  val sran_bts__startup_log__startup_rmod_check = List("line_number", "SN","Manufacturer","ProductCode","HW_Version","RMOD_Type","RMOD_LETTER_ID","RMOD_ID")
  val lte_oms__cat__ioms_mirroring_error= List("line_number", "MIRROR_STATE")
  val lte_oms__df__df_out = List("line_number", "FILESYSTEM","1K_BLOCKS","USED","AVAILABLE","USEDPERCENTAGE","MOUNTED_PATH")
  val lte_oms__zstatus__zstatus_out_alarmprocessor_check = List("alarm_id","mo","fs_alarm_processor_id",
    "fs_fragment_id","specific_problem","alarm_time","local_alarm_time","line_number")
  val lte_oms__alarms__alarms_alarmprocessor_check = List("month","day","time","specific_problem","mo","iinfo","alarm_time","line_number")
  val lte_oms__syslog__spontaneous_ioms_reboot = List("date_time","severity","unit","source","message","line_number")
  val wcdma_rnc__alarm_history__rnc_alarm_history = List("alarm_consecutive_number","rnc_id",
    "dumm1","alarm_label","date_time","severity","notification_type","unit","wcel","alarm_number",
    "alarm_description","set_by","set_at","suppl_info1","suppl_info2","line_number")
  val wcdma_rnc__dsp_mml_data__cells_configured_in_dsp_pool = List("configured_cells","line_number")
  val flexi_mr_bts_lte__cupl__parse_scf_xml = List("parameter","value","line_number")
  val sran_bts__runtime__fsp_136a_runtime = List("n_a","cpu","date","time","message","info","message1","message2","line_number")
  val flexi_mr_bts_lte__123d_rawalarmhistory__123d_rawalarmhistory = List("fault_status","date_time","fault_id","fault_desc","fault_source","unit",
    "rest_data","line_number")
  val wcdma_rnc__wbts_info__wbts_wcell_count= List("wbts_count","wcell_count","line_number")
  val mcrnc__sw_manage_install__sw_manage_install = List("error_conversion_script_execution","line_number")
  val mcrnc__fsip_switch_port__fsip_switch_port = List("fsip_switch_port_admin_state","line_number")
  val mcrnc__startuplog___dram_info = List("status","ddr_value","flash_value","line_number")
  val mcrnc__syslog__master_syslog_unit_text_data = List("date_time","severity","unit","info","ru","pname","lpid","pid","text","data","line_number")
  val flexi_mr_bts_lte__1011_blackbox__parse_blackbox = List("date","time","event","line_number")
  val flexi_mr_wcdma_bts__pm__any_module_pm = List("hex_code","module_id","time","int_value","severity","source","message","line_number")
 // val flexi_bsc__unit_information__zusi_unitinfo = List("line_number","BCSU", "VLAN", "STATE", "MTU", "Attr", "IP_ADDR", "Subnet")
  val flexi_bsc__unit_information__zusi_unitinfo = List("line_number","BSC","DTTM","UNIT","PHYS","STATE","LOCATION","INFO")
  val flexi_mr_wcdma_bts__1011_rawalarmhistory__x011_rawalarmhistory = List("fault_status","date_time","fault_id","fault_name",
    "fault_source","subunit","subunit2","detecting_unit","sicad1","sicad2","sicad3","sicad4","current","min","h","day","all","line_number")
  val flexi_bsc__adjacent_cell_data_check__zeat_adjaceny_check = List("seg_id","bsc","lac_1","mnc_2","mnc_1","ci_2","mcc_1","date_time",
    "same_bcch_message","ci_1","lac_2","mcc_2","duplicate_message","illegal_message","line_number")
  val flexi_bsc__system_configuration__zwti_p_configuration=List("line_number","UNIT","Param1","CARD_NAME","CARD_INDEX","TRACK_NO","PCU_CARD_INTERNAL_PCM_NAME","PCM_TYPE","PCM_NO")
  val flexi_mr_bts_lte__runtime_default__1011_runtime_default = List("line_number","runtime_default")
  val sran_bts__runtime_btsom__x011_runtime_btsom = List("line_number","runtime_line_id","board","board_id","mo","date","time","mo_class","severity","path","message")
  val flexi_mr_wcdma_bts__alarms_xml__alarms_xml = List("line_number","alarm_number","alarm_activity","severity","alarm_detail","alarm_detail_nbr","feature_code",
    "event_type","observation_time","cell_obs_time","cell_obs_index","id","type_plug_unit","unit_nbr","subunit_nbr")
  val sran_bts__011_filelistinfo_log__filelistinfo = List("line_number","file_permition","links","owner","group","file_size","month","day","time_year","file_name")
  val btsmed__imp_upgrade__imp_upgrade_check_rpm_info = List("line_number","rpm_info")
  val btsmed__imp__imp_check_extra_info_alarm_8524 = List("line_number","date","time","alarm8524_additional_text")
  val flexi_bsc____tsn_0911_swu_edgeport= List("line_number","swu_id","low_port","high_port")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeeo= List("line_number","BSC","DTTM","NPC","GMAC","DMAC","GMIC","DMIC","DISB","TIM","EEF","EPF","EOF","Param","HRI","HRL","HRU","AUT","ALT","AML","ACH","IAC","SAL","ASG","CSD","CSU","TGT","HDL","HUL","CLR","TTSAP","PTMP","SAPT","CTR","CTC","MINHTT","MAXHTT","MAXHTS","TCHFR","SCHFR","CNGT","CNGS","CS","CSR","PRDMHT","PRDMHS","PRDCFR","PRDCNG","HIFLVL","HIFSHR","PRDHIF","PRDBNT","SMBNT","EMBNT","GTUGT","BCSUL","LAPDL","MSSCF","MSSCS","ALFRT","ALHRT","ALSDC","DINHO","DEXDR","RXBAL","RXANT","ITCF","VDLS","MNDL","MNUL","FPHO","ISS","PRE","SBCNF","SBCNH","SBCN","SBCNAF","SBCNAH","RXTA","DEC","IHTA","TTRC","MTTR","SRW","PRECPL","PRECPD","PRECRP","PRECTO","PRECRD","DHP","DNP","DLP","UP1","UP2","UP3","UP4","BGSW1","BGSW2","BGSW3","QCATR","RABLL","RUBLL","BL02","BL03","BL03","BL12","BL13","BL14","BL23","BL24","RL02","RL03","RL04","RL12","RL13","RL14","RL23","RL24","EGIC","IEPH","SPL","IPND","CSUMP","PSUMP","BTON","BTOFF","PLTON","PLTOFF","CSPDP","PSPDP","CPLPDP","MPLPDP","SSME","AF4WFQ","AF3WFQ","AF2WFQ","AF1WFQ","BEWFQ","AAF4WF","AAF3WF","AAF2WF","AAF1WF","ABEWFQ","VPBE","VPAF1","VPAF2","VPAF3","VPAF4","VPEF","AUCS","AUPS","ACP","AMP","CLKS","SS","ADSCPM","ADSCPB")
  val fixed_network__final_11050a20_TSF__tsf_parsing_sample = List("line_number", "seq_number", "errortask", "errortype", "errorclass", "username", "buildname", "filename", "linenumber", "errorinfo")
  val flexi_bsc__ss7_network__zobi_sccpbroadcast_defination=List("line_number","BROADCAST_GROUPS_ASBG","BROADCAST_GROUPS_CSBG")
  val wcdma_rnc__ip_configuration_txt__zqri_network_if_data = List("unit","if_name","adm_state","mtu","frag","if_priority","if_type","p_add_type","p_source_ip","p_subnet","p_dest_ip","l_add_type","l_source_ip","l_subnet","l_dest_ip","line_number")
  val flexi_mr_wcdma_bts__1011_rawalarmhistory_txt__active_alarms = List("date_time","fault_id","fault_name","fault_source","subunit","subunit2","detecting_unit","sicad1","sicad2","sicad3","sicad4","current","min","h","day","all","line_number")
  val mcrnc__rnc_backplane_txt__rnc_backplane = List("line_number","lmp_param","sfp","enab_disab","left_param","state","right1","right2","right3")
  val flexi_bsc__bcsu_5_pcu2_e_12_logs__ssv_pcu_esw = List("line_number","bcsu","pcu","date","pq2_boot_image","pq2_ram_image","dsp_ram_image","dsp_diagnostics_image","dsp_boot_image")
  val flexi_bsc__bts_seg_and_trx_parameters__bts_dfr8k_mode = List("line_number","bcf","bts","dfr8k")
  val mcbsc__io_system__zw7i_fea_state = List("line_number","feature_code","feature_name","feature_state","feature_capacity")
  val wcdma_rnc__mxu_10_computer_logs_txt__mxu_computer_logs = List("line_number","caller","type","date","time","user_text","user_data")
  val flexi_mr_bts_lte__ram_fault_history__fault_2 = List("line_number","type","date","fault","fault_id")
  val flexi_mr_bts_lte__rawalarmhistory__alarm_1 = List("line_number","date_time","param","date_time1","fault_id","fault_description","fault_src","serial_number")
  val flexi_bsc__swu__mstp_configuration = List("line_number","name","revision","instance","vlan")
  val sran_bts__1011_blackbox__1011_blackbox = List("line_number","date_time","reason","reason_additional")
  val flexi_bsc__ip_configurations__zqwl = List("line_number","cha_num","bcf_num","trx_num","busbaudrate","type","index","q1_addr","equipment","equgen","state")
  val flexi_bsc__system_configuration__zwti_pi_piulocation = List("line_number","piu_type","piu_index","location","unit")
  val flexi_mr_wcdma_bts__amr_and_hspa_call_ru40_traceId1_moc__html_parsing = List("line_number","order","time","details")
  val flexi_bsc__switch_info_txt__zwyi = List("line_number","id","unit","if_name","ip","superv_unit","hw_id")
  val flexi_bsc__switch_info_txt__sg_test_table_pattern = List("line_number","bsc","date_time","unit","phy_state","location","info")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo_sran_mutecall = List("line_number","bcf","site_type","sbts_id","pacc","pdv")
  val mcbsc__bts_seg_and_trx_parameters__zeqo_bts_ibho = List("line_number","bcf","bts","name","param","param1")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__fb_zefo_mutecall = List("line_number","bcf","site_type","bts_site_subtype",
  "sbts_id","administrative_state","op_state","pacc","pdv")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeei_bcsu_usage = List("line_number","unit","trxs","lapd_telecom_link","lapd_om","real_tech")
  val bsc3i__switch_info__zw6g_topology = List("line_number","topology","bsc_type")
  val flexi_bsc__etxx_configuration__zdwq_abis_d_channel = List("line_number","name","num","int_id","sapi","tei","association_name","stream_number","host_unit","state")
  val flexi_bsc__bts_seg_and_trx_parameters__bts_dfca_mode = List("line_number","bcf","bts","dfca")
  val flexi_bsc__locked_files__ziwx_lfiles = List("line_number","file_name","file_version","file_no","file_length","data_length","creator","attr")
  val sran_bts__runtime_btsom__runtime_btsom_only_wrn = List("line_number","runtime_line_id","board","board_id","mo","date_time","mo_class","path","message")
  val flexi_bsc__ip_configurations_txt__fb_ipc_mc_etme_eep = List("line_number","etme","ip_part1","ip_part2","ip_part3","ip_part4","subnet")
  val flexi_bsc__supplementary_ss7_network__zocj = List("line_number","bsc","date_time","set_number","set_name","no","name","value","unit")
  val flexi_bsc__clock_and_lapd_status__zdsb_lapd_state = List("line_number","name","number","sapi","tei","bit_rate","external_pcm_tsl_sub_tsl","unit","term","term_function","log_term","internal_pcm_tsl","parameter_set")
  val flexi_bsc__measurement_status__ztpi_meas_state = List("line_number","bsc","date_time","type","last_modified_date","last_modified_time",
    "admin_state","oper_state","me_interval_mon_time","me_interval_tue_time","me_interval_wed_time","me_interval_thu_time","me_interval_fri_time",
    "me_interval_sat_time","me_interval_sun_time","start_date","stop_date","output_interval")
  val flexi_bsc__preprocessor_sw__zdpx_gsw_preproinput = List("line_number","bsc","date_time","disk_image","gsw","sw256","primary_image","backup_image")
  val flexi_bsc__supplementary_ss7_network__zodi = List("line_number","bsc","date_time","oosg","odsg","treatment")
  val flexi_bsc__ip_configurations__zqht = List("line_number","unit","tx_packets","rx_packets","broadcasts_tx","broadcasts_rx","vlan_tx","vlan_rx","errors","tx_carrier_sense_lost")
  val flexi_bsc__ip_configurations_txt__zqkb_statroute = List("line_number","unit","ip1","ip2","ip3","ip4","df","df_ip1","df_ip2","df_ip3","df_ip4")
  val flexi_bsc__database_status__zdbd_omu = List("line_number","bsc_name","date_time","database","occurance","dumping_or_loading","consist_disk1","consist_disk2","allowed_operations","log_wri_count","wri_err_count","log_empty_count","empty_err_count","free_disk_log_buff")
  val flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__zewo_output  = List("line_number","bsc_name","date_time","bcf_number","status","build_id","version","subdir","state","sw_master")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeei_bcsu = List("line_number","BSC Name","DTTM","UNIT","TRXS","D_CH TELECOM LINKS","D_CH OM LINKS","REAL TCHS","TOTAL TRX","TOTAL REAL TCH","HARDWARE SUPPORTED MAXIMUM TRX CAPACITY","HW AND SW SUPPORTED MAXIMUM TRX CAPACITY")
  val flexi_bsc__bsc_alarm_history_txt__zahp_alarmwithsupplentary=List("line_number","UNIT","DTTM","issuer","Notice_id","Alarm_id","Alarm_description","Supp_info")
  val flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo=List("line_number","BSC","DTTM","BCF","Type_of_Site","Ad_State","OPERATIONAL_state","RXDL","BBU","NTIM","BTIM","MASTER_BCF","Clock_Source","SENA","SM","T200F","T200S","TOPST","TRS2","PLU","PLR","RFSS","ETPGID","EBID","VLANID","ULTS","ULCIR","ULCBS","PACC","BU1","BU2","PL1","PL2","DLCIR","MEMWT","MBMWT","MMPS","ETMEID","BCF UP TO DATE","PDV","CU PANE IP","M PLANE IP","CSMUXP","PSMUXP","segment","BTS_Operational_State"
  )
  val flexi_bsc__ip_configurations__fb_ipc_internal_ip=List("line_number","BCSU","VLAN","STATE","MTU","Attr","IP ADDR")
  val flexi_bsc__zeei__zeei_et=List("line_number","BCF","BTS_TYPE","State","Op_State","BCSU","BCF_DCH_NAME","LINK_STATE","LAC","CI","BTS_ID","bts_state","bts_op_state","Param1","Param2","Param3","Param4","Network","TRX_id","ADMINSTATE","OPSTATE","FREQ","FRT","ETPCM","DETAILS")
  val flexi_bsc__bsc_alarm_history_txt__alarm_sup = List("line_number", "BSC", "Unit", "Alarm_Type", "DTTM", "Severity", "Not_type",
    "Param1", "Trans_id", "Alarm_id", "Alarm_text", "Supp1", "Supp2", "Supp3", "Supp4", "Supp5", "Supp6", "Supp7", "Supp8", "Supp9")
  val flexi_bsc__bts_alarm_history__zeoh_bts_alarm_history=List("line_number","Type","BSC_Name","BCF_ID","BTS_ID","Tag","DTTM")
  val flexi_bsc__bsc_alarm_history__data_science = List("orignal_log_file_name","parsed_log_file_name","bsc_identifier","bsc_unit","alarm_type","timestamp","severity","event_category","event_id","alarm_id","alarm_message","event_criticality","event")
  val fixed_network__alexx__error_record_alexx = List("report_received","remote_time" ,"seq_number", "error_task",  "error_type","error_class","user_name","build_name", "file_name","line_number","error_info","call_stack","line_num" )


  /** *****************************************
    * Capture variables with schema for log file
    * ******************************************
    */
  /** *****************************************
    * Create statements for each parser to make Row[RDD] and Schema
    * ******************************************
    */





  def getrddschema(parserName: String): StructType = {
    var schema = new StructType
    parserName.trim match {
      case "flexi_bsc__boot_info__zwdi_bootimage" =>
        val header: List[String] = flexi_bsc__boot_info__zwdi_bootimage
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__sw_configuration__zwqo_cr_omupack" =>
        val header: List[String] = flexi_bsc__sw_configuration__zwqo_cr_omupack
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ip_configurations__zqri_etpsig_m" | "flexi_bsc__ip_configurations__fb_ipc_etpsig_j" =>
        val header: List[String] = flexi_bsc__ip_configurations__zqri_etpsig_m
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bts_sw_lmu_and_abis_pool_data__zewl_bts_sw_pack" | "flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__cellularrnzewl" =>
        val header: List[String] = flexi_bsc__bts_sw_lmu_and_abis_pool_data__zewl_bts_sw_pack
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bsc_alarm_parameters__zabo_bsc_blk_alarm" =>
        val header: List[String] = flexi_bsc__bsc_alarm_parameters__zabo_bsc_blk_alarm
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__switch_info__training_switch_info" =>
        val header: List[String] = `flexi_bsc__switch info__training_switch_info`
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__unit_information__zudh_diagnosis_history" =>
        val header: List[String] = flexi_bsc__unit_information__zudh_diagnosis_history
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bsc_alarm__zahp_zaho_bsc_alarms"|"mcbsc_bsc__bsc_alarms__zahp_zaho_bsc_alarms"|"mcbsc__bsc_alarm__zahp_zaho_bsc_alarms"|"bsc3i__bsc_alarms__zahp_zaho_bsc_alarms" |"bsc3i__bsc_alarm__zahp_zaho_bsc_alarms"| "flexi_bsc__bsc_alarm_history_txt__alarm_sup_te" =>
        val header: List[String] = flexi_bsc__bsc_alarm__zahp_zaho_bsc_alarms
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bts_alarm_history__zeoh_bts_alarm_history" | "flexi_bsc__bts_alarm_history__zeoh_bts__alarm_history" | "bsc3i__bsc_alarm__zahp_zaho_bsc_alarms"|"flexi_bsc__bts_alarm_history_txt__zeoh_bts" =>
        val header: List[String] = flexi_bsc__bts_alarm_history__zeoh_bts_alarm_history
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__system_configuration__zwoi_pr_file" =>
        val header: List[String] = flexi_bsc__system_configuration__zwoi_pr_file
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__sw_configuration__zwqo_run_bscpack" | "flexi_bsc__sw_configuration__fb_unit_info_zusi" =>
        val header: List[String] = flexi_bsc__sw_configuration__zwqo_run_bscpack
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__system_configuration__zwos_pr_file" =>
        val header: List[String] = flexi_bsc__system_configuration__zwos_pr_file
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))

      case "flexi_bsc__bsc_alarm_history_txt__bsc_alarm" | "flexi_bsc__bsc_alarm_history__bsc_alarm" | "mcbsc__bsc_alarm_history__bsc_alarm" =>
        val header: List[String] = flexi_bsc__bsc_alarm_history_txt__bsc_alarm
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))

      case "flexi_bsc__system_configuration__zwti_c" =>
        val header: List[String] = flexi_bsc__system_configuration__zwti_c
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))

      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zego_bsc_timers" =>
        val header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data__zego_bsc_timers
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))

      case "flexi_bsc__oem_network__zqdi_max_osi_data" =>
        val header: List[String] = flexi_bsc__oem_network__zqdi_max_osi_data
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__io_system__zw7i_fea_state"|"flexi_bsc__io_system__zw7i_fea_states" =>
        val header: List[String] = flexi_bsc__io_system__zw7i_fea_state
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__database_status__zdbs_dbstatecheck" =>
        val header: List[String] = flexi_bsc__database_status__zdbs_dbstatecheck
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__io_system__zi2h_protocol_state" =>
        val header: List[String] = flexi_bsc__io_system__zi2h_protocol_state
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__cell_broadcast_status__zecp_cell_broadcast_msg" =>
        val header: List[String] = flexi_bsc__cell_broadcast_status__zecp_cell_broadcast_msg
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__utpfil_and_memory_files__fb_ipc_obsolete_utpfil" =>
        val header: List[String] = flexi_bsc__utpfil_and_memory_files__fb_ipc_obsolete_utpfil
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))

      case "flexi_bsc__bsc_alarm_history__zahp_bsc_alarm_histroy" =>
        var header: List[String] = flexi_bsc__bsc_alarm_history__zahp_bsc_alarm_histroy
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, true)))

      case "flexi_bsc__bsc_alarms__zaho_bsc_alarm_check" =>
        var header: List[String] = flexi_bsc__bsc_alarms__zaho_bsc_alarm_check
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, true)))

      case "flexi_bsc__bsc_alarm_history_txt__computer_logs" =>
        var header: List[String] = flexi_bsc__bsc_alarm_history_txt__computer_logs
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, true)))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeek_fbpp_status" | "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeek" =>

        var header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeek_fbpp_status
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, true)))
      case "flexi_bsc__measurement_status__zifi_event_sending_status" =>
        var header: List[String] = flexi_bsc__measurement_status__zifi_event_sending_status
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, true)))
      case "flexi_bsc__finaletpaping__ip_ping_results" | "flexi_bsc__finaletpaping_txt__packet_loss" =>
        var header: List[String] = flexi_bsc__finaletpaping__ip_ping_results
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__trx_deletion_error__zerd" =>
        var header: List[String] = flexi_bsc__trx_deletion_error__zerd
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, true)))
      /*  case "flexi_bsc__unit_information__zusi_unitinfo"=> {
          var header: List[String] = flexi_bsc__unit_information__zusi_unitinfo
          schema =
            StructType(header.map(fieldname => StructField(fieldname, StringType, true)))

        }
      */  /*case "flexi_bsc__bsc_alarm_history__bsc_alarm" => {
        var header: List[String] = flexi_bsc__bsc_alarm_history__bsc_alarm
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      }*/

      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_c_m_plane_ip" =>
        var header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_c_m_plane_ip
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, true)))


      case "flexi_bsc__system_configuration__zwti_u_configuration" =>
        val header: List[String] = flexi_bsc__system_configuration__zwti_u_configuration
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcrnc__rncsignaling__rncsignaling_ss7_association" =>
        val header: List[String] = mcrnc__rncsignaling__rncsignaling_ss7_association
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcbsc__bsc_alarms__zaho_bsc_alarm_check" =>
        var header: List[String] = mcbsc__bsc_alarms__zaho_bsc_alarm_check
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, true)))
      case "flexi_bsc__ip_configurations__zqri_bcsu_internal_ip" | "flexi_bsc__ip_configurations__fb_ipc_zqri_bcsu" =>
        var header: List[String] = flexi_bsc__ip_configurations__zqri_bcsu_internal_ip
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, true)))
      case "mcrnc__rnchw__rnchw_smartctl_hdd_grown_defect" | "mcrnc__rnchw__hdd_grown_defect" =>
        var header: List[String] = mcrnc__rnchw__rnchw_smartctl_hdd_grown_defect
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, true)))
      case "mcrnc__rncsignaling__rncsignaling_sccp_destination_point_code" =>
        val header: List[String] = mcrnc__rncsignaling__rncsignaling_sccp_destination_point_code
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcrnc__rncsignaling__rncsignalingsccpsubsystemremote" =>
        val header: List[String] = mcrnc__rncsignaling__rncsignalingsccpsubsystemremote
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "wcdma_rnc__omu_computer_logs__omu_computer_logs" =>
        val header: List[String] = wcdma_rnc__omu_computer_logs__omu_computer_logs
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeeo_bsc_dfca" =>
        val header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeeo_bsc_dfca
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__rawalarmhistory__mrbts_rawalarmhistory" =>
        val header: List[String] = flexi_mr_bts_lte__rawalarmhistory__mrbts_rawalarmhistory
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__computer_logs__zdde_computer_logs" =>
        var header: List[String] = flexi_bsc__computer_logs__zdde_computer_logs
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, true)))
      case "flexi_bsc__ip_configurations__zqri_ip_configuration" =>
        var header: List[String] = flexi_bsc__ip_configurations__zqri_ip_configuration
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, true)))
      case "mcrnc__rncrnw__radio_network_iuparams" =>
        val header: List[String] = mcrnc__rncrnw__radio_network_iuparams
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcrnc__rnchw__embedded_sw_status" | "mcrnc__rnchw__esw_status" =>
        val header: List[String] = mcrnc__rnchw__embedded_sw_status
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcrnc__rnchw__lmp_hw_status" =>
        val header: List[String] = mcrnc__rnchw__lmp_hw_status
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcrnc__rnchw__rnchwsfpstatus" =>
        val header: List[String] = mcrnc__rnchw__rnchwsfpstatus
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__1011_rawalarmhistory__parse_rawalarmhistory"|"sran_bts__1011_rawalarmhistory__parse_rawalarmhistory"|"flexi_mr_bts_lte__1011_rawalarmhistory__1011_rawalarmhistory"|"sran_bts__1011_rawalarmhistory__rawalarmhistory" =>
        val header: List[String] = flexi_mr_bts_lte__1011_rawalarmhistory__parse_rawalarmhistory
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__locked_files__ziwx_alhistgx" =>
        val header: List[String] = flexi_bsc__locked_files__ziwx_alhistgx
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__io_system__zw7i_fea_usage" =>
        val header: List[String] = flexi_bsc__io_system__zw7i_fea_usage
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__switch_info__zw6g_topology" =>
        val header: List[String] = flexi_bsc__switch_info__zw6g_topology
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__clock_and_lapd_status__zdti_lapd_check" =>
        val header: List[String] = flexi_bsc__clock_and_lapd_status__zdti_lapd_check
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__clock_and_lapd_status__zdri_sync_input" =>
        val header: List[String] = flexi_bsc__clock_and_lapd_status__zdri_sync_input
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ip_configurations__zqvi_pad_parameters" =>
        val header: List[String] = flexi_bsc__ip_configurations__zqvi_pad_parameters
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ss7_network_txt__znci_m3ua_based_signalling_links" =>
        val header: List[String] = flexi_bsc__ss7_network_txt__znci_m3ua_based_signalling_links
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_m_" =>
        val header: List[String] = flexi_bsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_m_
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bsc_alarm_history_txt__zahp_alarm_sup" | "flexi_bsc__bsc_alarm_history_txt__suntest" =>
        val header: List[String] = flexi_bsc__bsc_alarm_history_txt__zahp_alarm_sup
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__supplementary_ss7_network__zosk" =>
        val header: List[String] = flexi_bsc__supplementary_ss7_network__zosk
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__boot_info_txt__zwdi_boot_image_of_units" =>
        val header: List[String] = flexi_bsc__boot_info_txt__zwdi_boot_image_of_units
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ip_configurations__zqkb_static_routes" =>
        val header: List[String] = flexi_bsc__ip_configurations__zqkb_static_routes
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ip_configurations__zqkb_statroute" =>
        val header: List[String] = flexi_bsc__ip_configurations__zqkb_statroute
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__supervision_and_disk_status_txt__fb_unit_memory_check" =>
        val header: List[String] = flexi_bsc__supervision_and_disk_status_txt__fb_unit_memory_check
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeoe_bts_blocked_alarms" =>
        val header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeoe_bts_blocked_alarms
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__unit_information__fb_mc_unit_status" =>
        val header: List[String] = flexi_bsc__unit_information__fb_mc_unit_status
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bts_sw_lmu_and_abis_pool_data__zewo_bcf_swpack" =>
        val header: List[String] = flexi_bsc__bts_sw_lmu_and_abis_pool_data__zewo_bcf_swpack
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__swu_7__tsn_0911_swu_7_edgeport" | "flexi_bsc__swu_6_txt__edge_test" | "flexi_bsc__swu_6__tsn_0911_swu_6_edgeport" =>
        val header: List[String] = flexi_bsc__swu_7__tsn_0911_swu_7_edgeport
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__switch_info__switch_working_state" =>
        val header: List[String] = flexi_bsc__switch_info__switch_working_state
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__boot_info_txt__zwdi" =>
        val header: List[String] = flexi_bsc__boot_info_txt__zwdi
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeeo_dinho" =>
        val header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeeo_dinho
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__fb_bsc_bcf_mc_sbnt" =>
        val header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__fb_bsc_bcf_mc_sbnt
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bts_seg_and_trx_parameters__zeqo_basic1" =>
        val header: List[String] = flexi_bsc__bts_seg_and_trx_parameters__zeqo_basic1
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ss7_network__znci_signalling_link_data" =>
        val header: List[String] = flexi_bsc__ss7_network__znci_signalling_link_data
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_bcf_all_parameters1" | "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_bcf_all_parameters"| "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo_bcf_all_parameter" =>
        val header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_bcf_all_parameters1
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bts_seg_and_trx_parameters__zeqo_btsparameter" =>
        val header: List[String] = flexi_bsc__bts_seg_and_trx_parameters__zeqo_btsparameter
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__zewl_output" =>
        val header: List[String] = flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__zewl_output
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__preprocessor_sw__zdpp_cls_preproinput"=>
        val header: List[String] = flexi_bsc__preprocessor_sw__zdpp_cls_preproinput
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__extendedsysteminfo__extendedsysteminfo_diskusage"|"sran_bts__extendedsysteminfo_log__extendedsysteminfo_diskusage" =>
        val header: List[String] = sran_bts__extendedsysteminfo__extendedsysteminfo_diskusage
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__etxx_configuration__zwti_pp_etphotlink"=>
        val header: List[String] = flexi_bsc__etxx_configuration__zwti_pp_etphotlink
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__etxx_configuration__zesl_pcu_pcuonetp"=>
        val header: List[String] = flexi_bsc__etxx_configuration__zesl_pcu_pcuonetp
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__supplementary_ss7_network__zoyo"=>
        val header: List[String] = flexi_bsc__supplementary_ss7_network__zoyo
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_p_"=>
        val header: List[String] = flexi_bsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_p
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__scf__sbtstimezone" =>
        val header: List[String] = sran_bts__scf__sbtstimezone
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__scf__scf_pmax" =>
        val header: List[String] = sran_bts__scf__scf_pmax
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__dump_routing_table__routingtablenew"|"flexi_mr_bts_lte__dump_routing_table_txt__routingtablenew" =>
        val header: List[String] = sran_bts__dump_routing_table__routingtablenew
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__snapshot_file_list__snapshot_file_list_countofmodules" =>
        val header: List[String] = sran_bts__snapshot_file_list__snapshot_file_list_countofmodules
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__dump_routing_table__routingtable"|"sran_bts__dump_routing_table_txt__routingtable" =>
        val header: List[String] = sran_bts__dump_routing_table__routingtable
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__1011_rawalarmhistory__bts_1011_rawalarmhistory" =>
        val header: List[String] = sran_bts__1011_rawalarmhistory__bts_1011_rawalarmhistory
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc___ip_configurations" =>
        val header: List[String] = flexi_bsc___ip_configurations
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__snapshot_file_list__dsp_crash" =>
        val header: List[String] = sran_bts__snapshot_file_list__dsp_crash
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ss7_network__zobl_ip_broadcast" =>
        val header: List[String] = flexi_bsc__ss7_network__zobl_ip_broadcast
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ss7_network__zobi_sccpbroadcast_defination"=>
        val header: List[String] = flexi_bsc__ss7_network__zobi_sccpbroadcast_defination
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcrnc__rnchw__embedded_sw_version" | "mcrnc__rnchw__esw_version_check"=>
        val header: List[String] = mcrnc__rnchw__embedded_sw_version
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__scf__x2_link_status"=>
        val header: List[String] = sran_bts__scf__x2_link_status
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__011_rawalarmhistory__rawalarmhistory"=>
        val header: List[String] = sran_bts__011_rawalarmhistory__rawalarmhistory
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__scf__scf_dlmimotype"=>
        val header: List[String] = sran_bts__scf__scf_dlmimotype
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__runtime__rfmodulefaults"=>
        val header: List[String] = sran_bts__runtime__rfmodulefaults
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))

      case "mcrnc__rncalarm__mcrnc_active_alarm" | "mcrnc__rncalarm_txt__rnc_alarm"=>
        val header: List[String] = mcrnc__rncalarm__mcrnc_active_alarm
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))

      case "sran_bts__lrm_dump__lrm_dump"=>
        val header: List[String] = sran_bts__lrm_dump__lrm_dump
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))

      case "sran_bts__scf__scf_channelgrouping"=>
        val header: List[String] = sran_bts__scf__scf_channelgrouping
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__extendedsystem__cp_traffic_wcdma" =>
        val header: List[String] = sran_bts__extendedsystem__cp_traffic_wcdma
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "wcdma_rnc__functional_units__zusi_full" | "wcdma_rnc__functional_units__functional_units" =>
        val header: List[String] = wcdma_rnc__functional_units__zusi_full
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "wcdma_rnc__diagnostics__diagnostics_report_history_unit_diagnose_status" =>
        val header: List[String] = wcdma_rnc__diagnostics__diagnostics_report_history_unit_diagnose_status
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__trs__parse_ethlk" =>
        val header: List[String] = flexi_mr_bts_lte__trs__parse_ethlk
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "wcdma_rnc__active_alarms__rnc_active_alarm" =>
        val header: List[String] = wcdma_rnc__active_alarms__rnc_active_alarm
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__messages_2__read_messages_2" =>
        val header: List[String] = flexi_mr_bts_lte__messages_2__read_messages_2
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__runtime_btsom_log__1011_runtime" =>
        val header: List[String] = flexi_mr_bts_lte__runtime_btsom_log__1011_runtime
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__011_rawalarmhistory__011_rawalarmhistory_alldata" =>
        val header: List[String] = flexi_mr_bts_lte__011_rawalarmhistory__011_rawalarmhistory_alldata
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__tas_extended_startup__tas_extended_startup_lmpipcheck" =>
        val header: List[String] = sran_bts__tas_extended_startup__tas_extended_startup_lmpipcheck
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcbsc__mcbc___zw6t_flowcontrol" =>
        val header: List[String] = mcbsc__mcbc___zw6t_flowcontrol
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "lte_oms__df__df_out" =>
        val header: List[String] = lte_oms__df__df_out
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bts_alarms__zeol_bts_alarms" =>
        val header: List[String] = flexi_bsc__bts_alarms__zeol_bts_alarms
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ip__fb_ipc_zqri_etpxunit" =>
        val header: List[String] = flexi_bsc__ip__fb_ipc_zqri_etpxunit
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__scf__scf_parameters_all" =>
        val header: List[String] = sran_bts__scf__scf_parameters_all
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__scf_xml__fetch_lnrel" =>
        val header: List[String] = flexi_mr_bts_lte__scf_xml__fetch_lnrel
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__startup_log__startup_rmod_check" | "flexi_mr_bts_lte__startup__startup_rmod_check" =>
        val header: List[String] = sran_bts__startup_log__startup_rmod_check
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "lte_oms__cat__ioms_mirroring_error" =>
        val header: List[String] = lte_oms__cat__ioms_mirroring_error
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "lte_oms__zstatus__zstatus_out_alarmprocessor_check" =>
        val header: List[String] = lte_oms__zstatus__zstatus_out_alarmprocessor_check
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "lte_oms__alarms__alarms_alarmprocessor_check" =>
        val header: List[String] = lte_oms__alarms__alarms_alarmprocessor_check
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "lte_oms__syslog__spontaneous_ioms_reboot" =>
        val header: List[String] = lte_oms__syslog__spontaneous_ioms_reboot
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "wcdma_rnc__alarm_history__rnc_alarm_history" =>
        val header: List[String] = wcdma_rnc__alarm_history__rnc_alarm_history
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "wcdma_rnc__dsp_mml_data__cells_configured_in_dsp_pool" =>
        val header: List[String] = wcdma_rnc__dsp_mml_data__cells_configured_in_dsp_pool
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__cupl__parse_scf_xml" =>
        val header: List[String] = flexi_mr_bts_lte__cupl__parse_scf_xml
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__runtime__fsp_136a_runtime" | "flexi_mr_bts_lte__runtime__fsp_136a_runtime" | "flexi_mr_bts_lte__pm__pm"=>
        val header: List[String] = sran_bts__runtime__fsp_136a_runtime
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__123d_rawalarmhistory__123d_rawalarmhistory" =>
        val header: List[String] = flexi_mr_bts_lte__123d_rawalarmhistory__123d_rawalarmhistory
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "wcdma_rnc__wbts_info__wbts_wcell_count" =>
        val header: List[String] = wcdma_rnc__wbts_info__wbts_wcell_count
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcrnc__sw_manage_install__sw_manage_install" =>
        val header: List[String] = mcrnc__sw_manage_install__sw_manage_install
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcrnc__fsip_switch_port__fsip_switch_port" =>
        val header: List[String] = mcrnc__fsip_switch_port__fsip_switch_port
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcrnc__startuplog___dram_info" =>
        val header: List[String] = mcrnc__startuplog___dram_info
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcrnc__syslog__master_syslog_unit_text_data" =>
        val header: List[String] = mcrnc__syslog__master_syslog_unit_text_data
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__1011_blackbox__parse_blackbox" | "flexi_mr_bts_td_lte__1011_blackbox__enb_safeguard_restart"|"flexi_mr_bts_lte__flexi_mr_bts_lte_011_blackbox__011_blackbox"|"flexi_mr_bts_lte__1011_blackbox__011_blackbox" =>
        val header: List[String] = flexi_mr_bts_lte__1011_blackbox__parse_blackbox
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_wcdma_bts__pm__any_module_pm" | "flexi_mr_wcdma_bts__pm__any_module_pm1"=>
        val header: List[String] = flexi_mr_wcdma_bts__pm__any_module_pm
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__unit_information__zusi_unitinfo" =>
        val header: List[String] = flexi_bsc__unit_information__zusi_unitinfo
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_wcdma_bts__1011_rawalarmhistory__x011_rawalarmhistory"=>
        val header: List[String] = flexi_mr_wcdma_bts__1011_rawalarmhistory__x011_rawalarmhistory
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__adjacent_cell_data_check__zeat_adjaceny_check" =>
        val header: List[String] = flexi_bsc__adjacent_cell_data_check__zeat_adjaceny_check
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__system_configuration__zwti_p_configuration" =>
        val header: List[String] = flexi_bsc__system_configuration__zwti_p_configuration
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__swu__swu_monitor_esb24" =>
        val header: List[String] = flexi_bsc__swu__swu_monitor_esb24
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__runtime_default__1011_runtime_default" =>
        val header: List[String] = flexi_mr_bts_lte__runtime_default__1011_runtime_default
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__runtime_btsom__x011_runtime_btsom" | "sran_bts__runtime_default__x011_runtime_default" | "sran_bts__runtime_trsw__x011_runtime_trsw" | "sran_bts__runtime_btsom_log__fortestingonly_btsom"=>
        val header: List[String] = sran_bts__runtime_btsom__x011_runtime_btsom
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_wcdma_bts__alarms_xml__alarms_xml" =>
        val header: List[String] = flexi_mr_wcdma_bts__alarms_xml__alarms_xml
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__011_filelistinfo_log__filelistinfo" =>
        val header: List[String] = sran_bts__011_filelistinfo_log__filelistinfo
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "btsmed__imp_upgrade__imp_upgrade_check_rpm_info" =>
        val header: List[String] = btsmed__imp_upgrade__imp_upgrade_check_rpm_info
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "btsmed__imp__imp_check_extra_info_alarm_8524" =>
        val header: List[String] = btsmed__imp__imp_check_extra_info_alarm_8524
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bsc_alarms__zahp_zaho_bsc_alarms" =>
        val header: List[String] = flexi_bsc__bsc_alarms__zahp_zaho_bsc_alarms
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__etp_logs__etp_active_call" =>
        val header: List[String] = flexi_bsc__etp_logs__etp_active_call
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc____tsn_0911_swu_edgeport" | "bsc3i__swu__tsn_0911_swu_edgeport" =>
        val header: List[String] = flexi_bsc____tsn_0911_swu_edgeport
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeeo" =>
        val header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeeo
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "fixed_network__final_11050a20_TSF__tsf_parsing_sample" =>
        val header: List[String] = fixed_network__final_11050a20_TSF__tsf_parsing_sample
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "wcdma_rnc__ip_configuration_txt__zqri_network_if_data" =>
        val header: List[String] = wcdma_rnc__ip_configuration_txt__zqri_network_if_data
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_wcdma_bts__1011_rawalarmhistory_txt__active_alarms" =>
        val header: List[String] = flexi_mr_wcdma_bts__1011_rawalarmhistory_txt__active_alarms
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcrnc__rnc_backplane_txt__rnc_backplane" =>
        val header: List[String] = mcrnc__rnc_backplane_txt__rnc_backplane
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bcsu_5_pcu2_e_12_logs__ssv_pcu_esw" =>
        val header: List[String] = flexi_bsc__bcsu_5_pcu2_e_12_logs__ssv_pcu_esw
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bts_seg_and_trx_parameters__bts_dfr8k_mode" =>
        val header: List[String] = flexi_bsc__bts_seg_and_trx_parameters__bts_dfr8k_mode
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcbsc__io_system__zw7i_fea_state" =>
        val header: List[String] = mcbsc__io_system__zw7i_fea_state
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "wcdma_rnc__mxu_10_computer_logs_txt__mxu_computer_logs" =>
        val header: List[String] = wcdma_rnc__mxu_10_computer_logs_txt__mxu_computer_logs
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__ram_fault_history__fault_2" | "flexi_mr_bts_lte__fault_history__mrbts_ram_fault_history" =>
        val header: List[String] = flexi_mr_bts_lte__ram_fault_history__fault_2
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_bts_lte__rawalarmhistory__alarm_1" =>
        val header: List[String] = flexi_mr_bts_lte__rawalarmhistory__alarm_1
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__swu__mstp_configuration" | "flexi_bsc__swu__local_mstp_configuration" =>
        val header: List[String] = flexi_bsc__swu__mstp_configuration
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__1011_blackbox__1011_blackbox" =>
        val header: List[String] = sran_bts__1011_blackbox__1011_blackbox
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ip_configurations__zqwl" =>
        val header: List[String] = flexi_bsc__ip_configurations__zqwl
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__system_configuration__zwti_pi_piulocation" =>
        val header: List[String] = flexi_bsc__system_configuration__zwti_pi_piulocation
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_mr_wcdma_bts__amr_and_hspa_call_ru40_traceId1_moc__html_parsing" =>
        val header: List[String] = flexi_mr_wcdma_bts__amr_and_hspa_call_ru40_traceId1_moc__html_parsing
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__switch_info_txt__zwyi" =>
        val header: List[String] = flexi_bsc__switch_info_txt__zwyi
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__switch_info_txt__sg_test_table_pattern" =>
        val header: List[String] = flexi_bsc__switch_info_txt__sg_test_table_pattern
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo_sran_mutecall" =>
        val header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo_sran_mutecall
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "mcbsc__bts_seg_and_trx_parameters__zeqo_bts_ibho" =>
        val header: List[String] = mcbsc__bts_seg_and_trx_parameters__zeqo_bts_ibho
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__fb_zefo_mutecall" =>
        val header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__fb_zefo_mutecall
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeei_bcsu_usage" =>
        val header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeei_bcsu_usage
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "bsc3i__switch_info__zw6g_topology" =>
        val header: List[String] = bsc3i__switch_info__zw6g_topology
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__etxx_configuration__zdwq_abis_d_channel" =>
        val header: List[String] = flexi_bsc__etxx_configuration__zdwq_abis_d_channel
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bts_seg_and_trx_parameters__bts_dfca_mode" =>
        val header: List[String] = flexi_bsc__bts_seg_and_trx_parameters__bts_dfca_mode
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__locked_files__ziwx_lfiles" =>
        val header: List[String] = flexi_bsc__locked_files__ziwx_lfiles
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "sran_bts__runtime_btsom__runtime_btsom_only_wrn" =>
        val header: List[String] = sran_bts__runtime_btsom__runtime_btsom_only_wrn
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ip_configurations_txt__fb_ipc_mc_etme_eep" =>
        val header: List[String] = flexi_bsc__ip_configurations_txt__fb_ipc_mc_etme_eep
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__supplementary_ss7_network__zocj" =>
        val header: List[String] = flexi_bsc__supplementary_ss7_network__zocj
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__clock_and_lapd_status__zdsb_lapd_state" =>
        val header: List[String] = flexi_bsc__clock_and_lapd_status__zdsb_lapd_state
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__measurement_status__ztpi_meas_state" =>
        val header: List[String] = flexi_bsc__measurement_status__ztpi_meas_state
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__preprocessor_sw__zdpx_gsw_preproinput" =>
        val header: List[String] = flexi_bsc__preprocessor_sw__zdpx_gsw_preproinput
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__supplementary_ss7_network__zodi" =>
        val header: List[String] = flexi_bsc__supplementary_ss7_network__zodi
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ip_configurations__zqht" =>
        val header: List[String] = flexi_bsc__ip_configurations__zqht
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__ip_configurations_txt__zqkb_statroute" =>
        val header: List[String] = flexi_bsc__ip_configurations_txt__zqkb_statroute
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__database_status__zdbd_omu" =>
        val header: List[String] = flexi_bsc__database_status__zdbd_omu
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__zewo_output" =>
        val header: List[String] = flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__zewo_output
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeei_bcsu" =>
        val header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeei_bcsu
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
       case "flexi_bsc__bsc_alarm_history_txt__zahp_alarmwithsupplentary"  | "flexi_bsc__bsc_alarm_history_txt__suntest" =>
         val header: List[String] = flexi_bsc__bsc_alarm_history_txt__zahp_alarmwithsupplentary
         schema =
           StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
       case "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo"=>
        val header: List[String] = flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
       case "flexi_bsc__ip_configurations__fb_ipc_internal_ip"=>
        val header: List[String] = flexi_bsc__ip_configurations__fb_ipc_internal_ip
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__zeei__zeei_et"=>
        val header: List[String] = flexi_bsc__zeei__zeei_et
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bsc_alarm_history_txt__alarm_sup" =>
        val header: List[String] = flexi_bsc__bsc_alarm_history_txt__alarm_sup
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "flexi_bsc__bsc_alarm_history__data_science" =>
        val header: List[String] = flexi_bsc__bsc_alarm_history__data_science
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
      case "fixed_network__alexx__error_record_alexx" =>
        val header: List[String] = fixed_network__alexx__error_record_alexx
        schema =
          StructType(header.map(fieldname => StructField(fieldname, StringType, nullable = true)))
    }
    schema
  }

  def getrddrow(final_rdd: RDD[Map[String, String]], parserName: String, spark:SparkSession): RDD[Row] = {
    var finalrdd: RDD[Row] = spark.sparkContext.emptyRDD[Row]
    parserName.trim match {
      case "flexi_bsc__boot_info__zwdi_bootimage" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("FUNCTIONAL_UNIT"), a("PLUG_IN_UNIT_TYPE_INDEX"), a("BOOT_PACKAGE_FLASH_MEMORY"), a("BOOT_PACKAGE_DISK_FILE")))
      case "flexi_bsc__bts_sw_lmu_and_abis_pool_data__zewl_bts_sw_pack" | "flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__cellularrnzewl" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"), a("BUILD_ID"), a("TYPE"), a("REL_VER"), a("INITIAL"), a("MASTER_FILE"), a("SUBDIR"), a("CONNECTED_SITES")))
      case "flexi_bsc__bsc_alarm_parameters__zabo_bsc_blk_alarm" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("AlarmNo"), a("AlarmText"), a("Units")))
      case "flexi_bsc__switch_info__training_switch_info" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"), a("Unit"), a("Phy_State"), a("Location"), a("Info")))
      case "flexi_bsc__unit_information__zudh_diagnosis_history" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("BSC"), a("Unit"), a("Param1"), a("Param2"), a("Message")))
      case "flexi_bsc__bsc_alarm__zahp_zaho_bsc_alarms" | "mcbsc_bsc__bsc_alarms__zahp_zaho_bsc_alarms" | "mcbsc__bsc_alarm__zahp_zaho_bsc_alarms"|"bsc3i__bsc_alarms__zahp_zaho_bsc_alarms" |"bsc3i__bsc_alarm__zahp_zaho_bsc_alarms"| "flexi_bsc__bsc_alarm_history_txt__alarm_sup_te" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"), a("BSC"), a("Unit"), a("Alarm_Type"), a("DTTM"), a("Severity"), a("Not_type"),
          a("Param1"), a("Issuer"), a("Trans_id"), a("Alarm_id"), a("Alarm_text"), a("Supp1"), a("Supp2"), a("Supp3"),
          a("Supp4"), a("Supp5"), a("Supp6"), a("Supp7"), a("Supp8"), a("Supp9")))
      case "flexi_bsc__bts_alarm_history__zeoh_bts_alarm_history" | "flexi_bsc__bts_alarm_history__zeoh_bts__alarm_history" | "flexi_bsc__bts_alarm_history_txt__zeoh_bts" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"), a("Type"), a("BSC_Name"), a("BCF_ID"), a("BTS_ID"), a("Tag"), a("DTTM"),
          a("Sev"), a("Alarm_or_Cancel"), a("TRX_ID"), a("BTS_NAME"), a("Param"), a("Notification_ID"), a("Alarm_No"), a("Alarm_text"), a("Supp_Info")))
      case "flexi_bsc__system_configuration__zwoi_pr_file" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("PARAMETER_CLASS"), a("PARAMETER_NAME"), a("IDENTIFIER"), a("NAME_OF_PARAMETER"), a("VALUE"),
          a("CHANGE_POSSIBILITY")))
      case "flexi_bsc__sw_configuration__zwqo_run_bscpack" | "flexi_bsc__sw_configuration__fb_unit_info_zusi" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("WORKING_PACKAGES_IN_UNITS_PAGE"), a("MBA"), a("UNIT")
          , a("NAME"), a("STATUS"), a("PACKAGE_ID"), a("REP_ID")))
      case "flexi_bsc__system_configuration__zwos_pr_file" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("PARAMETER_CLASS"), a("PARAMETER_NAME"),
          a("IDENTIFIER"), a("NAME_OF_PARAMETER"), a("ACTIVATION_STATUS")))
      case "flexi_bsc__bsc_alarm_history_txt__bsc_alarm" | "flexi_bsc__bsc_alarm_history__bsc_alarm" | "mcbsc__bsc_alarm_history__bsc_alarm" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("BSC"), a("UNIT"), a("ALARM_TYPE"),a("DTTM"), a("SEVERITY"), a("NOT_TYPE"),
          a("PARAM1"), a("PARAM2"), a("TRANS_ID"), a("ALARM_ID"), a("ALARM_TEXT"), a("ALARM_SUP")))
      case "flexi_bsc__system_configuration__zwti_c" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("CART"), a("HALL"), a("LOC"), a("P1"), a("P2"), a("P3"), a("P4"), a("AG"), a("AL")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zego_bsc_timers" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"), a("PARAMETER"), a("VALUE"), a("PRESET_VALUE"), a("MODIFIABLE")))
      case "flexi_bsc__oem_network__zqdi_max_osi_data" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("AE_NAME"), a("APPL"), a("NETADDR"), a("STATE"), a("UNIT"), a("FAMID"), a("PROCID")
          , a("AP_TYPE"), a("AP_TITLE"), a("AEQ"), a("P_SELECTOR"), a("P_SELECTOR"), a("T_SELECTOR")))
      case "flexi_bsc__database_status__zdbs_dbstatecheck" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("BSC_NAME"),a("DTTM"), a("WO_DATABASE"), a("WO_OCCURANCE"), a("WO"),
          a("WO_STATE"), a("WO_SUBSTATE"), a("SP_DATABASE"), a("SP_OCCURANCE"), a("SP"), a("SP_STATE"), a("SP_SUBSTATE"), a("MEMORY_UPDATES_PREVENTED"),
          a("DISK_UPDATES_PREV"), a("TRANSACTION_OVER_FLOW"), a("BACKUP_IS_ON")))
      case "flexi_bsc__io_system__zi2h_protocol_state" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"), a("IPv4_SSH_ENABLED"), a("IPv4_SSH_PORT"), a("IPv6_SSH_ENABLED"), a("IPv6_SSH_PORT"), a("PRIVATE_RSA_KEY_NAME"),
          a("PRIVATE_DSA_KEY_NAME"), a("LOGIN_GRACE_TIME"), a("SFTP_ENABLED"), a("IPv4_FTP_ENABLED"), a("IPv4_FTP_PORT"), a("IPv6_FTP_ENABLED"), a("IPv6_FTP_PORT")))
      case "flexi_bsc__cell_broadcast_status__zecp_cell_broadcast_msg" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("MSG_INDEX"), a("MSG_ID"), a("MSG_CODE"), a("MSG_INFO"),
          a("REP_RATE"), a("GEP_SCOPE"), a("CODING_GROUP"), a("MC"), a("LANG_ALPH")))
      case "flexi_bsc__utpfil_and_memory_files__fb_ipc_obsolete_utpfil" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("MCMU"), a("FAMILY"), a("UTPFIL"), a("UTPFILval1"),
          a("UTPFILval2")))
      case "flexi_bsc__bsc_alarm_history__zahp_bsc_alarm_histroy" =>
        finalrdd = final_rdd.map(a => Row(a("history"), a("supp"), a("alarm_txt"), a("alarm_no"),
          a("notification_id"), a("cart"), a("issuer"), a("type"), a("param"), a("date_time"), a("alart_type"), a("unit"), a("bsc"),a("line_number")))
      case "flexi_bsc__bsc_alarms__zaho_bsc_alarm_check" =>
        finalrdd = final_rdd.map(a => Row(a("history"), a("supp"), a("alarm_txt"), a("alarm_no"),
          a("notification_id"), a("cart"), a("issuer"), a("type"), a("param"), a("date_time"), a("alart_type"), a("unit"), a("bsc"),a("line_number")))
      case "flexi_bsc__bsc_alarm_history_txt__computer_logs" =>
        finalrdd = final_rdd.map(a => Row(a("caller"), a("write_time"), a("parameters"), a("user_text"), a("user_date"), a("user_date1"), a("user_date2")
          ,a("line_number")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeek_fbpp_status" | "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeek" =>
        finalrdd = final_rdd.map(a => Row(a("rnw_plan_database_state"), a("rnw_configuration_id"), a("rnw_plan_configuration_id"),
          a("rnw_fallback_configuration_id"),a("line_number")))
      case "flexi_bsc__measurement_status__zifi_event_sending_status" =>
        finalrdd = final_rdd.map(a => Row(a("event_sending"),a("line_number")))
      case "flexi_bsc__finaletpaping__ip_ping_results" | "flexi_bsc__finaletpaping_txt__packet_loss" =>
        finalrdd = final_rdd.map(a => Row(a("ip"), a("packets_transmitted"), a("received"), a("packet_loss"), a("time"),a("line_number")))

      case "flexi_bsc__trx_deletion_error__zerd" =>
        finalrdd = final_rdd.map(a => Row(a("error"), a("error_desc"),a("line_number")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_c_m_plane_ip" =>
        finalrdd = final_rdd.map(a => Row(a("line_number"),a("bts_cu_plane_ip_address"),a("bts_m_plane_ip_address"),a("csubnet"),a("msubnet"),a("line_number")))
      case "flexi_bsc__system_configuration__zwti_u_configuration" =>
        finalrdd = final_rdd.map(a => Row(a("unit"), a("param1"), a("param2"), a("location"), a("master"),a("line_number")))
      case "mcbsc__bsc_alarms__zaho_bsc_alarm_check" =>
        finalrdd = final_rdd.map(a => Row(a("history"), a("supp"), a("alarm_txt"),
          a("alarm_no"), a("notification_id"), a("cart"), a("issuer"), a("type"), a("param"), a("date_time"), a("alart_type"), a("unit"), a("bsc"),a("line_number")))
      case "flexi_bsc__ip_configurations__zqri_bcsu_internal_ip" | "flexi_bsc__ip_configurations__fb_ipc_zqri_bcsu"=>
        finalrdd = final_rdd.map(a => Row(a("bcsu"),a("vlan1_ip_subnet"),a("vlan1_attr"),a("vlan1_mtu"),a("vlan1_state"),a("vlan1"),a("vlan2_ip_subnet"),
          a("vlan2_attr"),a("vlan2_mtu"),a("vlan2_state"),a("vlan2"),a("vlan3_ip_subnet"),a("vlan3_attr"),a("vlan3_mtu"),a("vlan3_state"),
          a("vlan3"),a("vlan4_ip_subnet"),a("vlan4_attr"),a("vlan4_mtu"),a("vlan4_state"),a("vlan4"),a("vlan5_ip_subnet"),a("vlan5_attr"),
          a("vlan5_mtu"),a("vlan5_state"),a("vlan5"),a("line_number")))
      case "mcrnc__rnchw__rnchw_smartctl_hdd_grown_defect" | "mcrnc__rnchw__hdd_grown_defect" =>
        finalrdd = final_rdd.map(a => Row(a("box"), a("smart_health_status"), a("elements_in_grown_defect_list"),a("line_number")))
      case "mcrnc__rncsignaling__rncsignaling_sccp_destination_point_code" => finalrdd = final_rdd.map(a => Row(
        a("line_number"), a("Point_code_Name"), a("Point_code_Id"), a("Remote_AS_Name"), a("Point_code"),
        a("SAP_Profile_Name"), a("PC_Type"), a("Status"), a("Include_PC_in_called_party_address"), a("SST_on_DPC_Accessible"),
        a("SCCP_Timer_Profile_Name")))
      case "mcrnc__rncsignaling__rncsignaling_ss7_association" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("association_id"), a("primary_local_ip_addr"), a("secondary_local_ip_addr"),
        a("local_client_port"), a("local_as_name"), a("vrf_name"), a("node"), a("remote_as_name"), a("primary_remote_ip_add"),
        a("secondary_remote_ip_addr"), a("remote_port"), a("exchange_mode"), a("sctp_profile"), a("communication_type"), a("admin_state"), a("role"),
        a("status"), a("active_path"), a("destination_reachable"), a("destination_reachable2")))
      case "mcrnc__rncsignaling__rncsignalingsccpsubsystemremote" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("SCCP_SubSystem_Identifier"),
        a("SCCP_SubSystem"), a("SCCP_SubSystem_Number"), a("SAP_Profile_Name"), a("Dest_Point_code_Name"), a("Concerned_SubSystem"),
        a("Status")))
      case "wcdma_rnc__omu_computer_logs__omu_computer_logs" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("CALLER"), a("WRITE_TIME"),
        a("PARAMETERS"), a("USER_TEXT"), a("USER_DATA"), a("USER_DATA1"), a("USER_DATA2")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeeo_bsc_dfca" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("EXPECTED_BSC_BSC_INTERFACE_DELAY")))
      case "flexi_mr_bts_lte__rawalarmhistory__mrbts_rawalarmhistory" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("Param"), a("DTTM"), a("FaultId"),
        a("Fault_Description"), a("FaultSrc"), a("SerialNumber")))
      case "flexi_bsc__computer_logs__zdde_computer_logs" => finalrdd = final_rdd.map(a => Row(a("line_number"),a("UNIT"), a("CALLER"), a("WRITE_TIME"), a("PARAMETERS"),
        a("USER_TEXT"), a("USER_DATA")))
        //"line_number","BSC", "DTTM","Unit","UNIT_PIU","Interface", "Interface1", "Attribute", "IP_ADD", "Interface2", "VLAN_ID", "STATE", "MTU")
      //case "flexi_bsc__ip_configurations__zqri_ip_configuration" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("BSC"), a("DTTM"), a("Unit"), a("UNIT_PIU"), a("VLAN_ID"), a("Interface"), a("Interface1"), a("Attribute"), a("IP_ADD"), a("STATE"), a("MTU"), a("Attr")))
      case "flexi_bsc__ip_configurations__zqri_ip_configuration" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("BSC"), a("DTTM"), a("Unit"), a("UNIT_PIU"), a("VLAN_ID"), a("Interface"), a("Interface1"), a("Attribute"), a("IP_ADD"), a("STATE"), a("MTU")))
      case "mcrnc__rncrnw__radio_network_iuparams" => finalrdd = final_rdd.map(a => Row(a("line_number"),a("INTERFACE"), a("IUId"), a("CNDomainVersion"), a("SignPointCode"), a("IuState"), a("IuLinkState"),
        a("IPBasedRouteId"), a("IPQMid"), a("DestIPAddress"), a("IPNetmask")))
      case "mcrnc__rnchw__embedded_sw_status" | "mcrnc__rnchw__esw_status" => finalrdd = final_rdd.map(a => Row(a("line_number"),a("CHASSIS"), a("FIRMWARE"), a("ACTIVE_MAINBANK"),
        a("ROLLBACK_BACKUPBANK"), a("PENDINGBANK")))
      case "mcrnc__rnchw__lmp_hw_status" => finalrdd = final_rdd.map(a => Row(a("line_number"),a("RNCid"),a("LMP_ID"), a("NODE_STATUS")))
      case "mcrnc__rnchw__rnchwsfpstatus" => finalrdd = final_rdd.map(a => Row(a("line_number"),a("LMPParam"), a("SFP_NAME"), a("ENAB_DISAB"), a("LEFTPARAM"), a("RIGHT1"),
        a("RIGHT2"), a("RIGHT3")))
      case "flexi_mr_bts_lte__1011_rawalarmhistory__parse_rawalarmhistory"|"sran_bts__1011_rawalarmhistory__parse_rawalarmhistory"|"flexi_mr_bts_lte__1011_rawalarmhistory__1011_rawalarmhistory"|"sran_bts__1011_rawalarmhistory__rawalarmhistory"=> finalrdd = final_rdd.map(a => Row(a("date"),a("time"),a("line_number"),a("Faults"), a("FAULT_ID"), a("FAULT_NAME"),a("SOURCE"), a("SOURCE_ADDITIONAL")))
      case "flexi_bsc__swu__swu_monitor_esb24" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("MIRROR"), a("PARAM1")))
      case "flexi_bsc__io_system__zw7i_fea_state"|"flexi_bsc__io_system__zw7i_fea_states" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("FEATURE_CODE"), a("FEATURE_NAME"), a("FEATURE_STATE"), a("FEATURE_CAPACITY")))
      case "flexi_bsc__ip_configurations__zqri_etpsig_m" | "flexi_bsc__ip_configurations__fb_ipc_etpsig_j" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("MCMU"), a("VLAN"), a("STATE"), a("MTU"),
        a("ATTR"), a("IP_ADDR")))
      //  case "flexi_bsc__unit_information__zusi_unitinfo" => finalrdd = final_rdd.map(a => Row(a("BSC"), a("DTTM"), a("UNIT"), a("PHYS"), a("STATE"), a("LOCATION"), a("INFO")))
      //      case "flexi_bsc__bsc_alarm__zahp_zaho_bsc_alarms" | "mcbsc__bsc_alarms__zahp_zaho_bsc_alarms" | "bsc3i__bsc_alarms__zahp_zaho_bsc_alarms" => finalrdd = final_rdd.map(a => Row(a("BSC"), a("UNIT"), a("ALARM_TYPE"), a("DTTM"), a("SEVERITY"), a("NOT_TYPE"), a("PARAM1")
      //        , a("ISSUER"), a("TRANS_ID"), a("ALARM_NO"), a("ALARM_TXT"), a("SUPP")))
      case "flexi_bsc__etp_logs__etp_active_call" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("NUM_CS_CALLS_ACTIVE"), a("ETP_ID")))
      case "flexi_bsc__bts_alarms__zeol_bts_alarms" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("BSC_NAME"), a("BCF_ID"), a("BTS_ID"), a("TAG"), a("DTTM"), a("SEVERITY"), a("ALARM_OR_CANCEL"), a("TRX_ID"),
        a("BTS_NAME"), a("NOTIFICATION_ID"), a("ALARM_NO"), a("ALARM_TXT"), a("SUPP_INFO")))
      case "flexi_bsc__sw_configuration__zwqo_cr_omupack" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("SW_PACKAGE"),
        a("STATUS"), a("ENV"), a("DIRECTORY"), a("DEF"), a("ACT")))
      case "flexi_bsc__locked_files__ziwx_alhistgx" => final_rdd.map(a => Row(a("line_number"), a("ALHISTGX-FILE_VERSION"), a("ALHISTGX-FILE_NO"),
        a("ALHISTGX-FILE_LENGTH")))
      case "flexi_bsc__io_system__zw7i_fea_usage" => finalrdd = final_rdd.map(a => Row(a("feature_code"), a("feature_usage"),a("line_number")))
      case "flexi_bsc__switch_info__zw6g_topology" => finalrdd = final_rdd.map(a => Row(a("topology"), a("bsc_type"),a("line_number")))
      case "flexi_bsc__clock_and_lapd_status__zdti_lapd_check" => finalrdd = final_rdd.map(a => Row(a("name"), a("num"), a("unit"), a("interface_side"),
        a("pcm_tsl_tsl"), a("sapi"), a("working_state"),a("line_number")))
      case "flexi_bsc__clock_and_lapd_status__zdri_sync_input" => finalrdd = final_rdd.map(a => Row(a("unit"), a("state"), a("used_input"), a("priority"),
        a("line_number")))
      case "flexi_bsc__ip_configurations__zqvi_pad_parameters" => finalrdd = final_rdd.map(a => Row(a("nr"), a("padp_name"), a("current_value"),
        a("line_number")))
      case "flexi_bsc__ss7_network_txt__znci_m3ua_based_signalling_links" => finalrdd = final_rdd.map(a => Row(a("m3ua_link"), a("m3ua_link_set"),
        a("association_set"), a("m3ua_param_set"),a("line_number")))
      case "flexi_bsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_m_" => finalrdd = final_rdd.map(a => Row(a("unit"),
        a("pool"), a("pool_percentage"), a("common_buffers"), a("common_buffers_percentage"), a("family_environment"),
        a("family_environment_percentage"), a("private_buffers"), a("private_buffers_percentage"), a("message_buffers"),
        a("message_buffers_percentage"), a("free_memory"), a("free_memory_percentage"), a("free_headers_count"),
        a("free_headers_count_percentage"),a("line_number")))
      case "flexi_bsc__bsc_alarm_history_txt__zahp_alarm_sup" | "flexi_bsc__bsc_alarm_history_txt__suntest" => finalrdd = final_rdd.map(a => Row(a("bsc"),
        a("supp"), a("alarm_txt"), a("alarm_id"), a("trans_id"), a("issuer"), a("param1"), a("not_type"),
        a("severity"), a("date_time"), a("unit"), a("alarm_type"),a("line_number")))
      case "flexi_bsc__boot_info_txt__zwdi" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("UNIT"), a("PIU_TYPE"), a("BOOT_PKG_FLASH"), a("FLASH_VERSION"), a("DATE"), a("RANDOM"), a(
        "BOOT_PKG_DISK"), a("DISK_VERSION"), a("DV_DATE"), a("RANDOM1")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeeo_dinho" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("DINHO")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__fb_bsc_bcf_mc_sbnt" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("BCFDATA"), a("CUPLANE_IP1"), a("CUPLANE_IP2"), a(
        "CUPLANE_IP3"), a("CUPLANE_IP4"), a("CUPLANE_SubnetMask"), a("MPLANE_IP1"), a("MPLANE_IP2"), a("MPLANE_IP3"), a("MPLANE_IP4"), a("MPLANE_SubnetMask")))
      case "flexi_bsc__supplementary_ss7_network__zosk"=> finalrdd = final_rdd.map(a => Row(a("mtpsccp"), a("sp_code_hd"), a("sp_name"),
        a("reporting_status"),a("line_number")))
      case "flexi_bsc__boot_info_txt__zwdi_boot_image_of_units"=> finalrdd = final_rdd.map(a => Row(a("line_number"), a("Functional_Unit"),
        a("Plug_In_Unit_Type_Index"), a("Boot_Package"), a("Flash_Memory"), a("Disk_File")))
      case "flexi_bsc__ip_configurations__zqkb_static_routes"=> finalrdd = final_rdd.map(a => Row(a("line_number"), a("UNIT"), a("IPROUTE"), a("PHY"),
        a("NUM"), a("DF")))
      case "flexi_bsc__ip_configurations__zqkb_statroute"=> finalrdd = final_rdd.map(a => Row(a("line_number"), a("UNIT"), a("IP1"), a("IP2"),
        a("IP3"), a("IP4"), a("DF"), a("DF_IP1"), a("DF_IP2"), a("DF_IP3"), a("DF_IP4"), a("DF")))
      case "flexi_bsc__supervision_and_disk_status_txt__fb_unit_memory_check"=> finalrdd = final_rdd.map(a => Row(a("unit")
        , a("pool"), a("free_memory"),a("line_number")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeoe_bts_blocked_alarms"=> finalrdd = final_rdd.map(a => Row(a("param1"),
        a("param2"), a("unit"), a("date_time"),a("line_number")))
      case "flexi_bsc__unit_information__fb_mc_unit_status"=> finalrdd = final_rdd.map(a => Row(a("unit"), a("info"),a("line_number")))
      case "flexi_bsc__bts_sw_lmu_and_abis_pool_data__zewo_bcf_swpack"=> finalrdd = final_rdd.map(a => Row(a("bcf_number"),
        a("nw_status"), a("nw_build_id"), a("nw_version"), a("nw_subdir"), a("nw_state"), a("nw_swmaster"),
        a("bu_status"), a("bu_build_id"), a("bu_version"), a("bu_subdir"), a("bu_state"), a("bu_swmaster"),
        a("fb_status"), a("fb_build_id"), a("fb_version"), a("fb_subdir"), a("fb_state"), a("fb_swmaster"),a("line_number")))
      case "flexi_bsc__swu_7__tsn_0911_swu_7_edgeport" | "flexi_bsc__swu_6__tsn_0911_swu_6_edgeport" | "flexi_bsc__swu_6_txt__edge_test"=>finalrdd = final_rdd.map(a => Row(a("low_port"), a("high_port"),a("line_number")))
      case "flexi_bsc__switch_info__switch_working_state"=>finalrdd = final_rdd.map(a => Row(a("unit"), a("phy_state"), a("location"), a("info"),a("line_number")))
      case "flexi_bsc__bts_seg_and_trx_parameters__zeqo_basic1"=> finalrdd = final_rdd.map(a => Row(a("line_number"),a("BCFNAME"), a("BTSNAME"), a("BSCNAME"),
        a("PARAMNAME"), a("SHORTNAME"), a("PARAMVALUE")))
      case "flexi_bsc__ss7_network__znci_signalling_link_data"=>finalrdd= final_rdd.map(a=>Row(a("line_number"), a("LINK"), a("LINK_SET"), a("PCM-TSL"), a("UNIT"), a("TERM"), a("TF"), a("LOG_UNIT"), a("LOG_TERM"), a("PAR_SET"),
        a("BIT_RATE"), a("MTP2_REQ")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_bcf_all_parameters1" | "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zefo_bcf_all_parameters" | "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo_bcf_all_parameter"=>finalrdd = final_rdd.map(a=>Row(a("bcf_number"),a("param_name"),
        a("param_value"),a("line_number")))
      case "flexi_bsc__bts_seg_and_trx_parameters__zeqo_btsparameter" => finalrdd= final_rdd.map(a=>Row(a("bcf"),
        a("bts"),a("bts_adm_state"),a("bts_state"),a("gena"),a("psei"),a("pcu_index"),a("pcu_id"),
        a("pcu_state"),a("bvci"),a("nsei1"),a("bvc1_state"),a("nsei2"),a("bvc2_state"),a("nsei3"),
        a("bvc3_state"),a("line_number")))
      case "flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__zewl_output" => finalrdd = final_rdd.map(a=>Row(a("bsc_name"),
        a("date_time"),a("build_id"),a("type"),a("rel_ver"),a("initial"),a("mf_name"),a("subdir"),a("conn"),a("line_number")))
      case "flexi_bsc__preprocessor_sw__zdpp_cls_preproinput"=> finalrdd = final_rdd.map(a=>Row(a("bsc"),
        a("date_time"),a("cls"),a("cls1"),a("rom_sw"),a("fpga_revision"),a("cpld_usercode"),a("line_number")))
      case "sran_bts__extendedsysteminfo__extendedsysteminfo_diskusage"|"sran_bts__extendedsysteminfo_log__extendedsysteminfo_diskusage" => finalrdd = final_rdd.map(a=>Row(a("file_system"),a("1k_blocks"),a("used"),
        a("available"),a("use_percentage"),a("mount_on"),a("line_number")))
      case "flexi_bsc__etxx_configuration__zwti_pp_etphotlink"=>finalrdd = final_rdd.map(a => Row(a("line_number"),a("UNIT"),a("PIU"),a("PORT"),a("UNIT_1"),a("PIU_1"),
        a("PORT_1")))
      case "flexi_bsc__etxx_configuration__zesl_pcu_pcuonetp"=>finalrdd = final_rdd.map(a => Row(a("line_number"),a("ETPx"),a("ETP_INDEX"),a("BCSU"),a("PCU")))
      case  "flexi_bsc__supplementary_ss7_network__zoyo"=>finalrdd = final_rdd.map(a => Row(a("line_number"),a("ParameterSetName"),a("SET"),a("RTOMIN"),a("RTOMAX")
        ,a("RTOINIT"),a("HBInterval"),a("SackPeriod"),a("PathMaxRET"),a("ASSMAXRET"),a("CHECKSUM"),a("BUNDLING")))
      case  "flexi_bsc__supervision_and_disk_status__zdoi_omu_mcmu_bcsu_p_"=>finalrdd = final_rdd.map(a => Row(a("line_number"),a("UNIT"),a("TIME_USAGE_ALLOWED"),a("LOAD_ALLOWED"),
        a("LOAD_PERCENT"),a("CLASS_FOR_CRRQ"),a("CLOCK_FREQUENCY_MHZ")))
      case "sran_bts__scf__sbtstimezone" => finalrdd = final_rdd.map(a=>Row(a("offset"),a("country"),a("line_number")))
      case "sran_bts__scf__scf_pmax"=> finalrdd= final_rdd.map(a=>Row(a("mrbts_id"),a("lnbts_id"),a("lncel_id"),a("line_number")))
      case "sran_bts__dump_routing_table__routingtablenew"|"flexi_mr_bts_lte__dump_routing_table_txt__routingtablenew" => finalrdd = final_rdd.map(a=>Row(a("param1"),a("param2"),a("param3"),a("param4"),a("param5"),
        a("param6"),a("param7"),a("param8"),a("gateway_ip"),a("rmtu"),a("pmtu"),a("otf"),a("used_flag"),a("line_number")))
      case "sran_bts__snapshot_file_list__snapshot_file_list_countofmodules" => finalrdd= final_rdd.map(a=>Row(a("param"),a("line_number")))
      case "sran_bts__dump_routing_table__routingtable"|"sran_bts__dump_routing_table_txt__routingtable" => finalrdd= final_rdd.map(a=>Row(a("src_ip"),a("dst_ip"),a("gateway_ip"),a("rmtu"),a("pmtu"),
        a("otf"),a("used_flag"),a("proto"),a("src_port_min"),a("src_port_max"),a("dst_port_min"),a("dst_port_max"),a("line_number")))
      case "sran_bts__1011_rawalarmhistory__bts_1011_rawalarmhistory" => finalrdd = final_rdd.map(a=>Row(a("type"),a("date"),a("time"),
        a("fault_id"),a("fault_name"),a("source"),a("source_additional"),a("line_number")))
      case "flexi_bsc__ztpi_meas_state__measurement_status" =>
        finalrdd= final_rdd.map(a=>Row(a("line_number"),a("BSC"),a("DTTM"),a("TYPE"),a("LAST_MODIFIED_DATE"),a("LAST_MODIFIED_TIME"),a("ADMIN_STATE"),a("OPR_STATE")))
      case "flexi_bsc___ip_configurations" =>
        finalrdd= final_rdd.map(a=>Row(a("line_number"), a("unit"),a("Txpackets") ,a("RxPackets"),a("BroadcastTX") ,a("BroadcastRX")
          ,a("VLANtx"),a("VLANrx"),a("Errors") ,a("TXcarrierSenseLost" )))
      case "flexi_bsc__ss7_network__zobl_ip_broadcast"  =>
        finalrdd= final_rdd.map(a=>Row(a("line_number"), a("CONCERNED_LOCAL_SUBSYSTEMS"),a("BROADCAST_GROUPS")))
      case "flexi_bsc__ss7_network__zobi_sccpbroadcast_defination"=>
        finalrdd= final_rdd.map(a=>Row(a("line_number"), a("BROADCAST_GROUPS_ASBG"),a("BROADCAST_GROUPS_CSBG")))

      /* case "mcrnc__rnchw__embedded_sw_version" =>
         finalrdd= final_rdd.map(a=>Row(a("line_number"), a("ALARM_ID"),a("SPECIFIC_PROBLEM"),a("MANAGED_OBJECT"),a("SEVERITY"),a("CLEARED"),a(
           "CLEARING"),a("ACKNOWLEDGED"),a("ACK_USER_ID"),a("ACK_TIME"),a("ALARM_TIME"),a("EVENT_TYPE"),a("APPLICATION"),
           a("IDENTIF_APPL_ADDL_INFO"),a("APPL_ADDL_INFO")))*/

      case "mcrnc__rnchw__embedded_sw_version" | "mcrnc__rnchw__esw_version_check" => finalrdd = final_rdd.map(a => Row(a("line_number"), a("FIRMWARE"), a("ACTIVE_MAINBANK"),
        a("ROLLBACK_BACKUPBANK"), a("PENDINGBANK"), a("BASELINE_BANK")))
      case "sran_bts__scf__x2_link_status" =>
        finalrdd= final_rdd.map(a=>Row(a("line_number"), a("LNADJ"),a("LINK_STATUS")))
      case "sran_bts__snapshot_file_list__dsp_crash" =>
        finalrdd= final_rdd.map(a=>Row(a("line_number"), a("PARAM1"),a("PARAM2")))
      case "sran_bts__scf__scf_dlmimotype" =>
        finalrdd= final_rdd.map(a=>Row(a("line_number"), a("MIMO_TYPE"),a("MRBTS_ID"),a("LNBTS_ID"),a("LNCEL_ID")))
      case "sran_bts__011_rawalarmhistory__rawalarmhistory" =>
        finalrdd= final_rdd.map(a=>Row(a("line_number"), a("DTTM"),a("TYPE"),a("FAULT_DTTM"),a("FAULT_ID"),a("FAULT_NAME"),a("SOURCE"),a("SOURCE_ADDITIONAL")))
      case "mcrnc__rncalarm__mcrnc_active_alarm" | "mcrnc__rncalarm_txt__rnc_alarm"=>finalrdd = final_rdd.map(a => Row(a("line_number"),a("Alarm_ID"),a("Specific_Problem"),a("Managed_Object"),a("Severity"),
        a("Cleared"),a("Clearing"),a("Acknowledged"),a("Ack_user_ID"),a("Ack_time"),a("Alarm_time"),a("Event_type"),a("Application"),a("Identif_appl_addl_info"),
        a("Appl_addl_info")))
      case "sran_bts__lrm_dump__lrm_dump"=>finalrdd = final_rdd.map(a => Row(a("line_number"),a("LCG_ID"),a("CaGroup_ID"),a("HspaConfig"),a("MulticastPSId"),a("MulticastSMId"),
        a("MulticastEVAMId"),a("StartAllDone"),a("HSUPA_Res_Allocation_Blocked"),a("MinNbrOfHsRachCfs"),a("ChannelCapacityExceeded_DCH_UL"),
        a("ChannelCapacityExceeded_DCH_DL"),a("ChannelCapacityExceeded_HSUPA_SM_1"),a("ChannelCapacityExceeded_HSUPA_SM_2"),
        a("ChannelCapacityExceeded_HSDPA")))
      case "sran_bts__scf__scf_channelgrouping"=>finalrdd = final_rdd.map(a => Row(a("line_number"),a("CELLMAPPING"),a("LCEL"),a("CHANNELGROUP"),a("CHANNEL"),a("DIRECTION"),
        a("MRBTS"),a("EQM"),a("APEQM"),a("RMOD"),a("ANTL")))
      case "sran_bts__runtime__rfmodulefaults"=> finalrdd = final_rdd.map(a => Row(a("line_number"),a("FaultName"),a("FaultId"),a("Source"),a("Severity"),a("State"),
        a("FaultCause")))
      case "sran_bts__extendedsystem__cp_traffic_wcdma" => finalrdd= final_rdd.map(a=>Row(a("packets"),a("bytes"),a("target"),a("protocol"),a("opt"),
        a("in"),a("out"),a("source"),a("destination"),a("additional_info"),a("line_number")))
      case "wcdma_rnc__functional_units__zusi_full" | "wcdma_rnc__functional_units__functional_units" => finalrdd = final_rdd.map(a=>Row(a("unit"),a("phys"),a("log"),a("state"),a("info"),a("line_number")))
      case "wcdma_rnc__diagnostics__diagnostics_report_history_unit_diagnose_status" => finalrdd = final_rdd.map(a=>Row(a("unit"),a("execution_status"),a("line_number")))
      case "flexi_mr_bts_lte__trs__parse_ethlk" =>  finalrdd = final_rdd.map(a=>Row(a("ethlk"),a("param"),a("value"),a("line_number")))
      case "wcdma_rnc__active_alarms__rnc_active_alarm" => finalrdd = final_rdd.map(a=>Row(a("alarm_consecutive_number"),
        a("rnc_id"),a("dumm1"),a("date_time"),a("severity"),a("notification_type"),a("unit"),a("wcel"),a("alarm_number"),
        a("alarm_description"),a("set_by"),a("set_at"),a("suppl_info1"),a("suppl_info2"),a("line_number")))
      case "flexi_mr_bts_lte__messages_2__read_messages_2" => finalrdd = final_rdd.map(a=>Row(a("date"),a("na"),a("error_type"),a("message"),a("line_number")))
      case "flexi_mr_bts_lte__runtime_btsom_log__1011_runtime" => finalrdd = final_rdd.map(a=>Row(a("runtime_btsom"),a("line_number")))
      case "flexi_mr_bts_lte__011_rawalarmhistory__011_rawalarmhistory_alldata" => finalrdd = final_rdd.map(a=>Row(a("fault_status"),
        a("date_time"),a("fault_id"),a("fault_name"),a("source"),a("fault_rised"),a("line_number")))
      case "sran_bts__tas_extended_startup__tas_extended_startup_lmpipcheck" =>
        finalrdd= final_rdd.map(a=>Row(a("line_number"), a("PARAM")))
      case "mcbsc__mcbc___zw6t_flowcontrol"=>
        finalrdd= final_rdd.map(a=>Row(a("line_number"), a("FLOWCONTROL")))
      case "lte_oms__df__df_out"=>
        finalrdd= final_rdd.map(a=>Row(a("line_number"), a("FILESYSTEM"),a("1K_BLOCKS"),a("USED"),a("AVAILABLE"),a("USEDPERCENTAGE"),a("MOUNTED_PATH")))
      case "flexi_bsc__ip__fb_ipc_zqri_etpxunit"=>
        finalrdd= final_rdd.map(a=>Row(a("line_number"), a("ETPxUNIT"),a("PARAM1"),a("PARAM2"),a("IP_ADDR"),a("SUBNET")))
      case "sran_bts__scf__scf_parameters_all"=>
        finalrdd= final_rdd.map(a=>Row(a("line_number"), a("param"),a("value")))
      case "flexi_mr_bts_lte__scf_xml__fetch_lnrel"=>  finalrdd= final_rdd.map(a=>Row(a("line_number"), a("LNREL"),a("PARAM"),a("VALUE")))
      case "sran_bts__startup_log__startup_rmod_check" | "flexi_mr_bts_lte__startup__startup_rmod_check"=>  finalrdd= final_rdd.map(a=>Row(a("line_number"), a("SN"),a("Manufacturer"),a("ProductCode"),
        a("HW_Version"),a("RMOD_Type"),a("RMOD_LETTER_ID"),a("RMOD_ID")))
      case "lte_oms__cat__ioms_mirroring_error"=> finalrdd= final_rdd.map(a=>Row(a("line_number"), a("MIRROR_STATE")))
      case "lte_oms__zstatus__zstatus_out_alarmprocessor_check" => finalrdd = final_rdd.map(a=>Row(a("alarm_id"),a("mo"),
        a("fs_alarm_processor_id"),a("fs_fragment_id"),a("specific_problem"),a("alarm_time"),a("local_alarm_time"),a("line_number")))
      case "lte_oms__alarms__alarms_alarmprocessor_check" => finalrdd = final_rdd.map(a=>Row(a("month"),a("day"),a("time"),
        a("specific_problem"),a("mo"),a("iinfo"),a("alarm_time"),a("line_number")))
      case "lte_oms__syslog__spontaneous_ioms_reboot" => finalrdd = final_rdd.map(a=>Row(a("date_time"),a("severity"),a("unit"),a("source"),
        a("message"),a("line_number")))
      case "wcdma_rnc__alarm_history__rnc_alarm_history" => finalrdd = final_rdd.map(a=>Row(a("alarm_consecutive_number"),a("rnc_id"),
        a("dumm1"),a("alarm_label"),a("date_time"),a("severity"),a("notification_type"),a("unit"),a("wcel"),a("alarm_number"),
        a("alarm_description"),a("set_by"),a("set_at"),a("suppl_info1"),a("suppl_info2"),a("line_number")))
      case "wcdma_rnc__dsp_mml_data__cells_configured_in_dsp_pool" => finalrdd = final_rdd.map(a=>Row(a("configured_cells"),a("line_number")))
      case "flexi_mr_bts_lte__cupl__parse_scf_xml" => finalrdd = final_rdd.map(a=>Row(a("parameter"),a("value"),a("line_number")))
      case "sran_bts__runtime__fsp_136a_runtime" | "flexi_mr_bts_lte__runtime__fsp_136a_runtime" | "flexi_mr_bts_lte__pm__pm"=> finalrdd= final_rdd.map(a=>Row(a("n_a"),a("cpu"),a("date"),a("time"),a("message"),
        a("info"),a("message1"),a("message2"),a("line_number")))
      case "flexi_mr_bts_lte__123d_rawalarmhistory__123d_rawalarmhistory" => finalrdd = final_rdd.map(a=>Row(a("fault_status"),
        a("date_time"),a("fault_id"),a("fault_desc"),a("fault_source"),a("unit"),a("rest_data"),a("line_number")))
      case "wcdma_rnc__wbts_info__wbts_wcell_count" => finalrdd = final_rdd.map(a=>Row(a("wbts_count"),a("wcell_count"),a("line_number")))
      case "mcrnc__sw_manage_install__sw_manage_install" => finalrdd = final_rdd.map(a=>Row(a("error_conversion_script_execution"),a("line_number")))
      case "mcrnc__fsip_switch_port__fsip_switch_port" => finalrdd = final_rdd.map(a=>Row(a("fsip_switch_port_admin_state"),a("line_number")))
      case "mcrnc__startuplog___dram_info" => finalrdd = final_rdd.map(a=>Row(a("status"),a("ddr_value"),a("flash_value"),a("line_number")))
      case "mcrnc__syslog__master_syslog_unit_text_data" => finalrdd = final_rdd.map(a=>Row(a("date_time"),a("severity"),a("unit"),a("info"),a("ru"),
        a("pname"),a("lpid"),a("pid"),a("text"),a("data"),a("line_number")))
      case "flexi_mr_bts_lte__1011_blackbox__parse_blackbox" | "flexi_mr_bts_td_lte__1011_blackbox__enb_safeguard_restart"|"flexi_mr_bts_lte__flexi_mr_bts_lte_011_blackbox__011_blackbox"|"flexi_mr_bts_lte__1011_blackbox__011_blackbox" => finalrdd = final_rdd.map(a=>Row(a("date"),a("time"),a("event"),a("line_number")))
      case "flexi_mr_wcdma_bts__pm__any_module_pm" | "flexi_mr_wcdma_bts__pm__any_module_pm1" => finalrdd = final_rdd.map(a=>Row(a("hex_code"),a("module_id"),a("time"),a("int_value"),a("severity"),
        a("source"),a("message"),a("line_number")))
/*      case "flexi_bsc__unit_information__zusi_unitinfo" => finalrdd = final_rdd.map(a =>
        Row(a("line_number"),a("BCSU"), a("VLAN"), a("STATE"), a("MTU"), a("Attr"), a("IP_ADDR"), a("Subnet")))*/
      case "flexi_bsc__unit_information__zusi_unitinfo" => finalrdd = final_rdd.map(a =>
        Row(a("line_number"),a("BSC"), a("DTTM"), a("UNIT"), a("PHYS"), a("STATE"), a("LOCATION"), a("INFO")))    
      case "flexi_mr_wcdma_bts__1011_rawalarmhistory__x011_rawalarmhistory" => finalrdd = final_rdd.map(a=>Row(a("fault_status"),a("date_time"),
        a("fault_id"),a("fault_name"),a("fault_source"),a("subunit"),a("subunit2"),a("detecting_unit"),a("sicad1"),a("sicad2"),a("sicad3"),
        a("sicad4"),a("current"),a("min"),a("h"),a("day"),a("all"),a("line_number")))
      case "flexi_bsc__adjacent_cell_data_check__zeat_adjaceny_check" => finalrdd = final_rdd.map(a=>Row(a("seg_id"),a("bsc"),a("lac_1"),a("mnc_2"),
        a("mnc_1"),a("ci_2"),a("mcc_1"),a("date_time"),a("same_bcch_message"),a("ci_1"),a("lac_2"),a("mcc_2"),a("duplicate_message"),a("illegal_message"),
        a("line_number")))
      case "flexi_bsc__system_configuration__zwti_p_configuration"=> finalrdd = final_rdd.map(a=>Row(a("line_number"),a("UNIT"),a("Param1"),a("CARD_NAME"),
        a("CARD_INDEX"),a("TRACK_NO"),a("PCU_CARD_INTERNAL_PCM_NAME"),a("PCM_TYPE"),a("PCM_NO")))
      case "flexi_mr_bts_lte__runtime_default__1011_runtime_default" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("runtime_default")))
      case "sran_bts__runtime_btsom__x011_runtime_btsom" | "sran_bts__runtime_default__x011_runtime_default" | "sran_bts__runtime_trsw__x011_runtime_trsw" | "sran_bts__runtime_btsom_log__fortestingonly_btsom"=> finalrdd = final_rdd.map(a=>Row(a("line_number"),a("runtime_line_id"),a("board"),a("board_id"),a("mo"),a("date"),a("time"),a("mo_class"),a("severity"),a("path"),a("message")))
      case "flexi_mr_wcdma_bts__alarms_xml__alarms_xml" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("alarm_number"),a("alarm_activity"),a("severity"),a("alarm_detail"),a("alarm_detail_nbr"),a("feature_code"),a("event_type"),a("observation_time"),a("cell_obs_time"),a("cell_obs_index"),a("id"),a("type_plug_unit"),a("unit_nbr"),a("subunit_nbr")))
      case "sran_bts__011_filelistinfo_log__filelistinfo" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("file_permition"),a("links"),a("owner"),a("group"),a("file_size"),a("month"),
        a("day"),a("time_year"),a("file_name")))
      case "btsmed__imp_upgrade__imp_upgrade_check_rpm_info" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("rpm_info")))
      case "btsmed__imp__imp_check_extra_info_alarm_8524" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("date"),a("time"),a("alarm8524_additional_text")))
      case "flexi_bsc__bsc_alarms__zahp_zaho_bsc_alarms" => finalrdd = final_rdd.map(a=>Row(a("line_number"), a("BSC"), a("UNIT"), a("ALARM_TYPE"), a("DTTM"),
        a("SEVERITY"), a("NOT_TYPE"), a("PARAM1"), a("ISSUER"), a("TRANS_ID"), a("ALARM_NO"), a("ALARM_TXT"), a("SUPP")))
      case "flexi_bsc____tsn_0911_swu_edgeport" | "bsc3i__swu__tsn_0911_swu_edgeport" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("swu_id"),a("low_port"),a("high_port")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeeo" => finalrdd = final_rdd.map(a=>Row(a("line_number"), a("BSC"), a("DTTM"), a("NPC"), a("GMAC"), a("DMAC"), a("GMIC"), a("DMIC"), a("DISB"), a("TIM"), a("EEF"), a("EPF"), a("EOF"), a("Param"), a("HRI"), a("HRL"), a("HRU"), a("AUT"), a("ALT"), a("AML"), a("ACH"), a("IAC"), a("SAL"), a("ASG"), a("CSD"), a("CSU"), a("TGT"), a("HDL"), a("HUL"), a("CLR"), a("TTSAP"), a("PTMP"), a("SAPT"), a("CTR"), a("CTC"), a("MINHTT"), a("MAXHTT"), a("MAXHTS"), a("TCHFR"), a("SCHFR"), a("CNGT"), a("CNGS"), a("CS"), a("CSR"), a("PRDMHT"), a("PRDMHS"), a("PRDCFR"), a("PRDCNG"), a("HIFLVL"), a("HIFSHR"), a("PRDHIF"), a("PRDBNT"), a("SMBNT"), a("EMBNT"), a("GTUGT"), a("BCSUL"), a("LAPDL"), a("MSSCF"), a("MSSCS"), a("ALFRT"), a("ALHRT"), a("ALSDC"), a("DINHO"), a("DEXDR"), a("RXBAL"), a("RXANT"), a("ITCF"), a("VDLS"), a("MNDL"), a("MNUL"), a("FPHO"), a("ISS"), a("PRE"), a("SBCNF"), a("SBCNH"), a("SBCN"), a("SBCNAF"), a("SBCNAH"), a("RXTA"), a("DEC"), a("IHTA"), a("TTRC"), a("MTTR"), a("SRW"), a("PRECPL"), a("PRECPD"), a("PRECRP"), a("PRECTO"), a("PRECRD"), a("DHP"), a("DNP"), a("DLP"), a("UP1"), a("UP2"), a("UP3"), a("UP4"), a("BGSW1"), a("BGSW2"), a("BGSW3"), a("QCATR"), a("RABLL"), a("RUBLL"), a("BL02"), a("BL03"), a("BL03"), a("BL12"), a("BL13"), a("BL14"), a("BL23"), a("BL24"), a("RL02"), a("RL03"), a("RL04"), a("RL12"), a("RL13"), a("RL14"), a("RL23"), a("RL24"), a("EGIC"), a("IEPH"), a("SPL"), a("IPND"), a("CSUMP"), a("PSUMP"), a("BTON"), a("BTOFF"), a("PLTON"), a("PLTOFF"), a("CSPDP"), a("PSPDP"), a("CPLPDP"), a("MPLPDP"), a("SSME"), a("AF4WFQ"), a("AF3WFQ"), a("AF2WFQ"), a("AF1WFQ"), a("BEWFQ"), a("AAF4WF"), a("AAF3WF"), a("AAF2WF"), a("AAF1WF"), a("ABEWFQ"), a("VPBE"), a("VPAF1"), a("VPAF2"), a("VPAF3"), a("VPAF4"), a("VPEF"), a("AUCS"), a("AUPS"), a("ACP"), a("AMP"), a("CLKS"), a("SS"), a("ADSCPM"), a("ADSCPB")))
      case "fixed_network__final_11050a20_TSF__tsf_parsing_sample" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("seq_number"), a("errortask"), a("errortype"), a("errorclass"), a("username"), a("buildname"), a("filename"), a("linenumber"), a("errorinfo")))
      case "wcdma_rnc__ip_configuration_txt__zqri_network_if_data" => finalrdd = final_rdd.map(a=>Row(a("unit"),a("if_name"),a("adm_state"),a("mtu"),a("frag"),a("if_priority"),a("if_type"),a("p_add_type"),a("p_source_ip"),a("p_subnet"),a("p_dest_ip"),a("l_add_type"),a("l_source_ip"),a("l_subnet"),a("l_dest_ip"),a("line_number")))
      case "flexi_mr_wcdma_bts__1011_rawalarmhistory_txt__active_alarms" => finalrdd = final_rdd.map(a=>Row(a("date_time"),a("fault_id"),a("fault_name"),a("fault_source"),a("subunit"),a("subunit2"),a("detecting_unit"),a("sicad1"),a("sicad2"),a("sicad3"),a("sicad4"),a("current"),a("min"),a("h"),a("day"),a("all"),a("line_number")))
      case "mcrnc__rnc_backplane_txt__rnc_backplane" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("lmp_param"),a("sfp"),a("enab_disab"),a("left_param"),a("state"),a("right1"),a("right2"),a("right3")))
      case "flexi_bsc__bcsu_5_pcu2_e_12_logs__ssv_pcu_esw" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bcsu"),a("pcu"),a("date"),a("pq2_boot_image"),a("pq2_ram_image"),a("dsp_ram_image"),a("dsp_diagnostics_image"),a("dsp_boot_image")))
      case "flexi_bsc__bts_seg_and_trx_parameters__bts_dfr8k_mode" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bcf"),a("bts"),a("dfr8k")))
      case "mcbsc__io_system__zw7i_fea_state" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("feature_code"),a("feature_name"),a("feature_state"),a("feature_capacity")))
      case "wcdma_rnc__mxu_10_computer_logs_txt__mxu_computer_logs" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("caller"),a("type"),a("date"),a("time"),a("user_text"),a("user_data")))
      case "flexi_mr_bts_lte__ram_fault_history__fault_2" | "flexi_mr_bts_lte__fault_history__mrbts_ram_fault_history" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("type"),a("date"),a("fault"),a("fault_id")))
      case "flexi_mr_bts_lte__rawalarmhistory__alarm_1" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("date_time"),a("param"),a("date_time1"),a("fault_id"),a("fault_description"),a("fault_src"),a("serial_number")))
      case "flexi_bsc__swu__mstp_configuration" | "flexi_bsc__swu__local_mstp_configuration" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("name"),a("revision"),a("instance"),a("vlan")))
      case "sran_bts__1011_blackbox__1011_blackbox" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("date_time"),a("reason"),a("reason_additional")))
      case "flexi_bsc__ip_configurations__zqwl" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("cha_num"),a("bcf_num"),a("trx_num"),a("busbaudrate"),a("type"),a("index"),a("q1_addr"),a("equipment"),a("equgen"),a("state")))
      case "flexi_bsc__system_configuration__zwti_pi_piulocation" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("piu_type"),a("piu_index"),a("location"),a("unit")))
      case "flexi_mr_wcdma_bts__amr_and_hspa_call_ru40_traceId1_moc__html_parsing" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("order"),a("time"),a("details")))
      case "flexi_bsc__switch_info_txt__zwyi" => finalrdd= final_rdd.map(a=>Row(a("line_number"),a("id"),a("unit"),a("if_name"),a("ip"),a("superv_unit"),a("hw_id")))
      case "flexi_bsc__switch_info_txt__sg_test_table_pattern" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bsc"),a("date_time"),a("unit"),a("phy_state"),a("location"),a("info")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo_sran_mutecall" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bcf"),a("site_type"),a("sbts_id"),a("pacc"),a("pdv")))
      case "mcbsc__bts_seg_and_trx_parameters__zeqo_bts_ibho" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bcf"),a("bts"),a("name"),a("param"),a("param1")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__fb_zefo_mutecall" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bcf"),a("site_type"),a("bts_site_subtype"),a("sbts_id"),a("administrative_state"),a("op_state"),a("pacc"),a("pdv")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zeei_bcsu_usage" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("unit"),a("trxs"),a("lapd_telecom_link"),a("lapd_om"),a("real_tech")))
      case "bsc3i__switch_info__zw6g_topology" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("topology"),a("bsc_type")))
      case "flexi_bsc__etxx_configuration__zdwq_abis_d_channel" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("name"),a("num"),a("int_id"),a("sapi"),a("tei"),a("association_name"),a("stream_number"),a("host_unit"),a("state")))
      case "flexi_bsc__bts_seg_and_trx_parameters__bts_dfca_mode" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bcf"),a("bts"),a("dfca")))
      case "flexi_bsc__locked_files__ziwx_lfiles" => finalrdd= final_rdd.map(a=>Row(a("line_number"),a("file_name"),a("file_version"),a("file_no"),a("file_length"),a("data_length"),a("creator"),a("attr")))
      case "sran_bts__runtime_btsom__runtime_btsom_only_wrn" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("runtime_line_id"),a("board"),a("board_id"),a("mo"),a("date_time"),a("mo_class"),a("path"),a("message")))
      case "flexi_bsc__ip_configurations_txt__fb_ipc_mc_etme_eep" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("etme"),a("ip_part1"),a("ip_part2"),a("ip_part3"),a("ip_part4"),a("subnet")))
      case "flexi_bsc__supplementary_ss7_network__zocj" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bsc"),a("date_time"),a("set_number"),a("set_name"),a("no"),a("name"),a("value"),a("unit")))
      case "flexi_bsc__clock_and_lapd_status__zdsb_lapd_state" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("name"),a("number"),a("sapi"),a("tei"),a("bit_rate"),a("external_pcm_tsl_sub_tsl"),a("unit"),a("term"),a("term_function"),a("log_term"),a("internal_pcm_tsl"),a("parameter_set")))
      case "flexi_bsc__measurement_status__ztpi_meas_state" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bsc"),a("date_time"),a("type"),
        a("last_modified_date"),a("last_modified_time"),a("admin_state"),a("oper_state"),a("me_interval_mon_time"),a("me_interval_tue_time"),
        a("me_interval_wed_time"),a("me_interval_thu_time"),a("me_interval_fri_time"),a("me_interval_sat_time"),a("me_interval_sun_time"),a("start_date"),
        a("stop_date"),a("output_interval")))
      case "flexi_bsc__preprocessor_sw__zdpx_gsw_preproinput" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bsc"),a("date_time"),a("disk_image"),a("gsw"),a("sw256"),a("primary_image"),a("backup_image")))
      case "flexi_bsc__supplementary_ss7_network__zodi" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bsc"),a("date_time"),a("oosg"),a("odsg"),a("treatment")))
      case "flexi_bsc__ip_configurations__zqht" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("unit"),a("tx_packets"),a("rx_packets"),a("broadcasts_tx"),a("broadcasts_rx"),a("vlan_tx"),a("vlan_rx"),a("errors"),a("tx_carrier_sense_lost")))
      case "flexi_bsc__ip_configurations_txt__zqkb_statroute" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("unit"),a("ip1"),a("ip2"),a("ip3"),a("ip4"),a("df"),a("df_ip1"),a("df_ip2"),a("df_ip3"),a("df_ip4")))
      case "flexi_bsc__database_status__zdbd_omu" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bsc_name"),a("date_time"),a("database"),
        a("occurance"),a("dumping_or_loading"),a("consist_disk1"),a("consist_disk2"),a("allowed_operations"),a("log_wri_count"),a("wri_err_count"),
        a("log_empty_count"),a("empty_err_count"),a("free_disk_log_buff")))
      case "flexi_bsc__bts_sw_lmu_and_abis_pool_data_txt__zewo_output" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("bsc_name"),a("date_time"),a("bcf_number"),a("status"),a("build_id"),a("version"),a("subdir"),a("state"),a("sw_master")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data__zeei_bcsu" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("BSC Name"),a("DTTM"),a("UNIT"),a("TRXS"),a("D_CH TELECOM LINKS"),a("D_CH OM LINKS"),a("REAL TCHS"),a("TOTAL TRX"),a("TOTAL REAL TCH"),a("HARDWARE SUPPORTED MAXIMUM TRX CAPACITY"),a("HW AND SW SUPPORTED MAXIMUM TRX CAPACITY")))
      case "flexi_bsc__bsc_alarm_history_txt__zahp_alarmwithsupplentary" | "flexi_bsc__bsc_alarm_history_txt__suntest" =>finalrdd = final_rdd.map(a=>Row(a("line_number"),a("UNIT"),a("DTTM"),a("issuer"),a("Notice_id"),a("Alarm_id"),a("Alarm_description"),a("Supp_info")))
      case "flexi_bsc__rnw_status_and_bsc_and_bcf_data_txt__zefo"=> finalrdd = final_rdd.map(a=>Row(a("line_number"),a("BSC"),a("DTTM"),a("BCF"),a("Type_of_Site"),a("Ad_State"),a("OPERATIONAL_state"),a("RXDL"),a("BBU"),a("NTIM"),a("BTIM"),a("MASTER_BCF"),a("Clock_Source"),a("SENA"),a("SM"),a("T200F"),a("T200S"),a("TOPST"),a("TRS2"),a("PLU"),a("PLR"),a("RFSS"),a("ETPGID"),a("EBID"),a("VLANID"),a("ULTS"),a("ULCIR"),a("ULCBS"),a("PACC"),a("BU1"),a("BU2"),a("PL1"),a("PL2"),a("DLCIR"),a("MEMWT"),a("MBMWT"),a("MMPS"),a("ETMEID"),a("BCF UP TO DATE"),a("PDV"),a("CU PANE IP"),a("M PLANE IP"),a("CSMUXP"),a("PSMUXP"),a("segment"),a("BTS_Operational_State")))
      case "flexi_bsc__ip_configurations__fb_ipc_internal_ip"=> finalrdd = final_rdd.map(a=>Row(a("line_number"),a("BCSU"),a("VLAN"),a("STATE"),a("MTU"),a("Attr"),a("IP ADDR")))
      case "flexi_bsc__zeei__zeei_et" => finalrdd = final_rdd.map(a=>Row(a("line_number"),a("BCF"),a("BTS_TYPE"),a("State"),a("Op_State"),a("BCSU"),a("BCF_DCH_NAME"),a("LINK_STATE"),a("LAC"),a("CI"),a("BTS_ID"),a("bts_state"),a("bts_op_state"),a("Param1"),a("Param2"),a("Param3"),a("Param4"),a("Network"),a("TRX_id"),a("ADMINSTATE"),a("OPSTATE"),a("FREQ"),a("FRT"),a("ETPCM"),a("DETAILS")))
      case "flexi_bsc__bsc_alarm_history_txt__alarm_sup"=> finalrdd = final_rdd.map(a => Row(a("line_number"), a("BSC"), a("Unit"), a("Alarm_Type"), a("DTTM"), a("Severity"), a("Not_type"),
          a("Param1"),  a("Trans_id"), a("Alarm_id"), a("Alarm_text"), a("Supp1"), a("Supp2"), a("Supp3"),
          a("Supp4"), a("Supp5"), a("Supp6"), a("Supp7"), a("Supp8"), a("Supp9")))
      case "flexi_bsc__bsc_alarm_history__data_science" => finalrdd = final_rdd.map(a=>Row(a("orignal_log_file_name"),a("parsed_log_file_name"),a("bsc_identifier"),a("bsc_unit"),a("alarm_type"),a("timestamp"),a("severity"),a("event_category"),a("event_id"),a("alarm_id"),a("alarm_message"),a("event_criticality"),a("event")))
      case "fixed_network__alexx__error_record_alexx" => finalrdd = final_rdd.map(a=>Row(a("report_received"),a("remote_time"),a("seq_number"),a("error_task"),a("error_type"),a("error_class"),a("user_name"),a("build_name"),a("file_name"),a("line_number"),a("error_info"),a("call_stack"),a("line_num")))
    }
    finalrdd
  }
}