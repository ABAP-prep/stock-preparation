CLASS zprepare_stock_for_test DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.

    CLASS-METHODS prepare_mchb_stock
      IMPORTING
        posting_date TYPE budat DEFAULT sy-datum
        target_batch_stocks TYPE mchb_tty
        logger TYPE REF TO zif_logger.

    CLASS-METHODS prepare_mska_stock
      IMPORTING
        posting_date TYPE budat DEFAULT sy-datum
        target_order_stocks TYPE zmska_tty
        logger TYPE REF TO zif_logger.

  PROTECTED SECTION.
  PRIVATE SECTION.
    TYPES: BEGIN OF _current_stock,
      matnr TYPE matnr,
      werks TYPE werks_d,
      lgort TYPE lgort_d,
      charg TYPE charg_d,
      clabs TYPE labst,
      cinsm TYPE insme,
      cspem TYPE speme,
      meins TYPE meins,
    END OF _current_stock.
    TYPES: BEGIN OF _current_order_stock,
      matnr TYPE matnr,
      werks TYPE werks_d,
      lgort TYPE lgort_d,
      charg TYPE charg_d,
      sobkz TYPE sobkz,
      vbeln TYPE vbeln,
      posnr TYPE posnr,
      kalab TYPE labst,
      kains TYPE insme,
      kaspe TYPE speme,
      meins TYPE meins,
    END OF _current_order_stock.
    CONSTANTS: BEGIN OF move_type,
      goods_receipt TYPE bwart VALUE '561',
      goods_issue TYPE bwart VALUE '562',
    END OF move_type.
    CONSTANTS: BEGIN OF stock_type,
      quality TYPE mb_insmk VALUE 'X',
      blocked TYPE mb_insmk VALUE 'S',
    END OF stock_type.
ENDCLASS.



CLASS ZPREPARE_STOCK_FOR_TEST IMPLEMENTATION.


  METHOD prepare_mchb_stock.
    DATA: goodsmvt_items TYPE bapi2017_gm_item_create_t,
          current_stocks TYPE HASHED TABLE OF _current_stock
          WITH UNIQUE KEY matnr werks lgort charg,
          move_type_diff TYPE bwart,
          messages TYPE bapiret2_t.

    IF target_batch_stocks IS INITIAL.
      RETURN.
    ENDIF.
    ##TOO_MANY_ITAB_FIELDS
    SELECT s~matnr, s~werks, s~lgort, s~charg, s~clabs, s~cinsm, s~cspem, m~meins
      FROM mchb AS s INNER JOIN mara AS m ON m~matnr = s~matnr
      FOR ALL ENTRIES IN @target_batch_stocks
      WHERE s~matnr = @target_batch_stocks-matnr
      AND s~werks = @target_batch_stocks-werks
      AND s~charg = @target_batch_stocks-charg
      AND s~lgort = @target_batch_stocks-lgort
      INTO CORRESPONDING FIELDS OF TABLE @current_stocks.

    LOOP AT target_batch_stocks REFERENCE INTO DATA(batch_stock).
      ##ENH_OK
      DATA(diff) = CORRESPONDING _current_stock( batch_stock->* ).
      READ TABLE current_stocks REFERENCE INTO DATA(current_stock)
        WITH TABLE KEY matnr = batch_stock->*-matnr
          werks = batch_stock->*-werks
          lgort = batch_stock->*-lgort
          charg = batch_stock->*-charg.
      IF sy-subrc = 0.
        diff-clabs = diff-clabs - current_stock->*-clabs.
        diff-cinsm = diff-cinsm - current_stock->*-cinsm.
        diff-cspem = diff-cspem - current_stock->*-cspem.
      ENDIF.

      IF diff-clabs <> 0.
        move_type_diff = COND bwart( WHEN diff-clabs > 0 THEN move_type-goods_receipt
          ELSE move_type-goods_issue ).
        INSERT VALUE #( material = batch_stock->*-matnr
          plant = batch_stock->*-werks batch = batch_stock->*-charg
          stge_loc = batch_stock->*-lgort move_type = move_type_diff
          entry_qnt = abs( diff-clabs ) entry_uom = diff-meins )
          INTO TABLE goodsmvt_items.
      ENDIF.
      IF diff-cinsm <> 0.
        move_type_diff = COND bwart( WHEN diff-cinsm > 0 THEN move_type-goods_receipt
          ELSE move_type-goods_issue ).
        INSERT VALUE #( material = batch_stock->*-matnr
          plant = batch_stock->*-werks batch = batch_stock->*-charg
          stge_loc = batch_stock->*-lgort move_type = move_type_diff
          entry_qnt = abs( diff-cinsm ) entry_uom = diff-meins
          stck_type = stock_type-quality )
          INTO TABLE goodsmvt_items.
      ENDIF.
      IF diff-cspem <> 0.
        move_type_diff = COND bwart( WHEN diff-cspem > 0 THEN move_type-goods_receipt
          ELSE move_type-goods_issue ).
        INSERT VALUE #( material = batch_stock->*-matnr
          plant = batch_stock->*-werks batch = batch_stock->*-charg
          stge_loc = batch_stock->*-lgort move_type = move_type_diff
          entry_qnt = abs( diff-cspem ) entry_uom = diff-meins
          stck_type = stock_type-blocked )
          INTO TABLE goodsmvt_items.
      ENDIF.

    ENDLOOP.
    IF goodsmvt_items IS INITIAL.
      RETURN.
    ENDIF.

    CALL FUNCTION 'BAPI_GOODSMVT_CREATE'
      EXPORTING
        goodsmvt_header = VALUE bapi2017_gm_head_01( pstng_date = posting_date )
        goodsmvt_code = VALUE bapi2017_gm_code( gm_code = '05' )
      TABLES
        goodsmvt_item = goodsmvt_items
        return = messages.
     logger->add( messages ).

  ENDMETHOD.


  METHOD prepare_mska_stock.
    DATA: current_stocks TYPE HASHED TABLE OF _current_order_stock
          WITH UNIQUE KEY matnr werks lgort charg sobkz vbeln posnr,
          move_type_diff TYPE bwart,
          goodsmvt_items TYPE bapi2017_gm_item_create_t,
          messages TYPE bapiret2_t.

    IF target_order_stocks IS INITIAL.
      RETURN.
    ENDIF.
    ##TOO_MANY_ITAB_FIELDS
    SELECT s~matnr, s~werks, s~lgort, s~sobkz, s~vbeln, s~posnr, s~charg, s~kalab, s~kains, s~kaspe, m~meins
      FROM mska AS s INNER JOIN mara AS m ON m~matnr = s~matnr
      FOR ALL ENTRIES IN @target_order_stocks
      WHERE s~matnr = @target_order_stocks-matnr
      AND s~werks = @target_order_stocks-werks
      AND s~charg = @target_order_stocks-charg
      AND s~lgort = @target_order_stocks-lgort
      INTO CORRESPONDING FIELDS OF TABLE @current_stocks.

    LOOP AT target_order_stocks REFERENCE INTO DATA(order_stock).
      ##ENH_OK
      DATA(diff) = CORRESPONDING _current_order_stock( order_stock->* ).
      READ TABLE current_stocks REFERENCE INTO DATA(current_stock)
        WITH TABLE KEY matnr = order_stock->*-matnr
          werks = order_stock->*-werks
          lgort = order_stock->*-lgort
          charg = order_stock->*-charg
          sobkz = order_stock->*-sobkz
          vbeln = order_stock->*-vbeln
          posnr = order_stock->*-posnr.
      IF sy-subrc = 0.
        diff-kalab = diff-kalab - current_stock->*-kalab.
        diff-kains = diff-kains - current_stock->*-kains.
        diff-kaspe = diff-kaspe - current_stock->*-kaspe.
      ENDIF.

      IF diff-kalab <> 0.
        move_type_diff = COND bwart( WHEN diff-kalab > 0 THEN move_type-goods_receipt
          ELSE move_type-goods_issue ).
        INSERT VALUE #( material = order_stock->*-matnr
          plant = order_stock->*-werks batch = order_stock->*-charg
          val_sales_ord = order_stock->*-vbeln val_s_ord_item = order_stock->*-posnr
          stge_loc = order_stock->*-lgort move_type = move_type_diff
          entry_qnt = abs( diff-kalab ) entry_uom = diff-meins
          spec_stock = order_stock->*-sobkz )
          INTO TABLE goodsmvt_items.
      ENDIF.
      IF diff-kains <> 0.
        move_type_diff = COND bwart( WHEN diff-kains > 0 THEN move_type-goods_receipt
          ELSE move_type-goods_issue ).
        INSERT VALUE #( material = order_stock->*-matnr
          plant = order_stock->*-werks batch = order_stock->*-charg
          val_sales_ord = order_stock->*-vbeln val_s_ord_item = order_stock->*-posnr
          stge_loc = order_stock->*-lgort move_type = move_type_diff
          entry_qnt = abs( diff-kains ) entry_uom = diff-meins
          stck_type = stock_type-quality spec_stock = order_stock->*-sobkz )
          INTO TABLE goodsmvt_items.
      ENDIF.
      IF diff-kaspe <> 0.
        move_type_diff = COND bwart( WHEN diff-kaspe > 0 THEN move_type-goods_receipt
          ELSE move_type-goods_issue ).
        INSERT VALUE #( material = order_stock->*-matnr
          plant = order_stock->*-werks batch = order_stock->*-charg
          val_sales_ord = order_stock->*-vbeln val_s_ord_item = order_stock->*-posnr
          stge_loc = order_stock->*-lgort move_type = move_type_diff
          entry_qnt = abs( diff-kaspe ) entry_uom = diff-meins
          stck_type = stock_type-blocked spec_stock = order_stock->*-sobkz )
          INTO TABLE goodsmvt_items.
      ENDIF.

    ENDLOOP.
    IF goodsmvt_items IS INITIAL.
      RETURN.
    ENDIF.

    CALL FUNCTION 'BAPI_GOODSMVT_CREATE'
      EXPORTING
        goodsmvt_header = VALUE bapi2017_gm_head_01( pstng_date = posting_date )
        goodsmvt_code = VALUE bapi2017_gm_code( gm_code = '05' )
      TABLES
        goodsmvt_item = goodsmvt_items
        return = messages.
     logger->add( messages ).

  ENDMETHOD.
ENDCLASS.
