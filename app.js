const dotenv  = require('dotenv');
const mysql   = require('mysql2/promise');
const fs = require('fs');

mergitionShop()

async function mergitionCrm()
{
  try 
  {
    let prod = {
      host    : "localhost",//process.env.DATABASE_CRM_HOST,
      database: "",
      user    : "",
      password: "",
      connectionLimit: 5,
      port: 3309
    }

    let newProd = {
      host: 'localhost',
      user: 'root',
      password: '00998877',
      database: 'xplay_crm'
    }

    let prodDb =  await mysql.createPool(prod);
    let newProdDb =  await mysql.createPool(newProd);

    let selectQueriesCrm = GenerateSelectQueriesCrm();

    const batchSize = 100000

    let start = 0


      for(let query of selectQueriesCrm.queries)
      {
        if (query.name != "user_otp"){  // select table
          continue
        }

        const startTime = new Date();
        const [result] = await prodDb.execute(query.query + ` LIMIT 0, 1`);
        const totalRows = result[0].rowCount;
        const qr = query.query.replace(", COUNT(*) AS rowCount" , "");

            for (start = 0; start < totalRows; start += batchSize) 
            {

              try 
              {
                const [rows] = await prodDb.execute(qr + ` LIMIT ${start}, ${batchSize}`);

                let insertColumns = Object.keys(rows[0])

                const values = rows.map(row => '(' + insertColumns.map(column => newProdDb.escape(row[column])).join(',') + ')').join(',');

                const queryInsert = `INSERT IGNORE INTO ${query.name} (${insertColumns}) VALUES ${values}`

                await newProdDb.execute(queryInsert);

                const progress = Math.min(((start + batchSize) / totalRows) * 100, 100);

                console.log(`Progress for table ${query.name}: ${progress.toFixed(2)}% Index : ${start}`);
                
              }
              catch (error) 
              {
                  if(error.code === "PROTOCOL_CONNECTION_LOST")
                  {
                    console.log("try connect");
                    prodDb = await mysql.createPool(prod);
                    newProdDb = await mysql.createPool(newProd);
                    start = start - batchSize
                    
                  }else{
                    console.log({error , stopIndex : start});
                    break;
                  }
              }

          } 

          const endTime = new Date();
          const executionTime = endTime - startTime; // Time difference in milliseconds

          const seconds = Math.floor(executionTime / 1000);
          const minutes = Math.floor(seconds / 60);
          const hours = Math.floor(minutes / 60);

          console.log(`Migration completed in ${hours} hor , ${minutes} min , ${seconds} sec , table : ${query.name}`);
        
      }
    
  } 
  catch (error) 
  {
    console.log("ERROR_CONNECTION_POOL: " + error);
  }

  console.log("END !");
  
}

async function mergitionFms()
{
  try 
  {
    let prod = {
      host    : "localhost",//process.env.DATABASE_CRM_HOST,
      database: "",
      user    : "",
      password: "",
      connectionLimit: 5,
      port: 3313 
  }

    let newProd = {
      host: 'localhost',
      user: 'root',
      password: '',
      database: 'xplay_fms'
    }

    let prodDb =  await mysql.createPool(prod);
    let newProdDb =  await mysql.createPool(newProd);

    let selectQueriesCrm = GenerateSelectQueriesFms();

    const batchSize = 100000

    let start = 0


      for(let query of selectQueriesCrm.queries)
      {
        // if (query.name != "tabby_history"){  // you can select table
        //   continue
        // }

        const [result] = await prodDb.execute(query.query + ` LIMIT 0, 1`);
        const totalRows = result[0].rowCount;
        const qr = query.query.replace(", COUNT(*) AS rowCount" , "");

            for (start = 0; start < totalRows; start += batchSize) 
            {

              try 
              {
                const [rows] = await prodDb.execute(qr + ` LIMIT ${start}, ${batchSize}`);

                let insertColumns = Object.keys(rows[0])

                const values = rows.map(row => '(' + insertColumns.map(column => newProdDb.escape(row[column])).join(',') + ')').join(',');

                const queryInsert = `INSERT INTO ${query.name} (${insertColumns}) VALUES ${values}`

                await newProdDb.execute(queryInsert);

                const progress = Math.min(((start + batchSize) / totalRows) * 100, 100);

                console.log(`Progress for table ${query.name}: ${progress.toFixed(2)}% Index : ${start}`);
                
              }
              catch (error) 
              {
                  if(error.code === "PROTOCOL_CONNECTION_LOST")
                  {
                    console.log("try connect");
                    prodDb = await mysql.createPool(prod);
                    newProdDb = await mysql.createPool(newProd);
                    start = start - batchSize
                    
                  }else{
                    console.log({error , stopIndex : start});
                    break;
                  }
              }

          } 

          console.log(`migration into table ${query.name} : ${start} / ${totalRows}`)
        
      }
    
  } 
  catch (error) 
  {
    console.log("ERROR_CONNECTION_POOL: " + error);
  }

  console.log("END !");
  
}

async function mergitionOms()
{
  try 
  {
    let prod = {
      host    : "localhost",//process.env.DATABASE_CRM_HOST,
      database: "xplay_oms",
      user    : "",
      password: "",
      connectionLimit: 5,
      port: 3311
  }

    let newProd = {
      host: 'localhost',
      user: 'root',
      password: '',
      database: 'xplay_oms'
    }

    const startTime = new Date();
    let prodDb =  await mysql.createPool(prod);
    let newProdDb =  await mysql.createPool(newProd);

    let selectQueriesCrm = GenerateSelectQueriesOms();

    const batchSize = 100000

    let start = 0

      for(let query of selectQueriesCrm.queries)
      {

        // if (query.name != "order_product"){  // you can select table
        //   continue
        // }

        const [result] = await prodDb.execute(query.query + ` LIMIT 0, 1`);
        const totalRows = result[0].rowCount;
        const qr = query.query.replace(", COUNT(*) AS rowCount" , "");

            for (start = 0; start < totalRows; start += batchSize) 
            {

              try 
              {
                let [rows] = await prodDb.execute(qr + ` LIMIT ${start}, ${batchSize}`);

                let insertColumns = Object.keys(rows[0])

                const values = rows.map(row => '(' + insertColumns.map(column => newProdDb.escape(row[column])).join(',') + ')').join(',');

                const queryInsert = "INSERT IGNORE INTO `"+query.name+"` ("+ insertColumns +") VALUES " + values

                await newProdDb.execute(queryInsert);

                const progress = Math.min(((start + batchSize) / totalRows) * 100, 100);

                console.log(`Progress for table ${query.name}: ${progress.toFixed(2)}% Index : ${start}`);
                
              }
              catch (error) 
              {
                  if(error.code === "PROTOCOL_CONNECTION_LOST")
                  {
                    console.log("try connect");
                    prodDb = await mysql.createPool(prod);
                    newProdDb = await mysql.createPool(newProd);
                    start = start - batchSize
                    
                  }else{
                    console.log({error , stopIndex : start});
                    break;
                  }
              }

          } 

          const endTime = new Date();
          const executionTime = endTime - startTime; // Time difference in milliseconds

          const seconds = Math.floor(executionTime / 1000);
          const minutes = Math.floor(seconds / 60);
          const hours = Math.floor(minutes / 60);

          console.log(`Migration completed in ${hours} hor , ${minutes} min , ${seconds} sec , table : ${query.name}`);
        
      }
    
  } 
  catch (error) 
  {
    console.log("ERROR_CONNECTION_POOL: " + error);
  }

  console.log("END !");
  
}

async function mergitionShop()
{
  try 
  {
    let prod = {
      host    : "localhost",//process.env.DATABASE_CRM_HOST,
      database: "xplay_cms",
      user    : "",
      password: "",
      port: 3314
  }

    let newProd = {
      host: 'localhost',
      user: '',
      password: '',
      database: 'xplay_shop',
      port : 3324
    }

    let prodDb =  await mysql.createPool(prod);
    let newProdDb =  await mysql.createPool(newProd);

    let selectQueriesCrm = GenerateSelectQueriesShop();

    const batchSize = 10000

    let start = 0

      for(let query of selectQueriesCrm.queries)
      {
        // if (query.name != "product_option_type"){  // you can select table
        //   continue
        // }

        const [result] = await prodDb.execute(query.query + ` LIMIT 0, 1`);
        const totalRows = result[0].rowCount;
        const qr = query.query.replace(", COUNT(*) AS rowCount" , "");

            for (start = 0; start < totalRows; start += batchSize) 
            {

              try 
              {
                const [rows] = await prodDb.execute(qr + ` LIMIT ${start}, ${batchSize}`);

                let insertColumns = Object.keys(rows[0])

                const values = rows.map(row => '(' + insertColumns.map(column => newProdDb.escape(row[column])).join(',') + ')').join(',');

                const queryInsert = "INSERT IGNORE INTO `"+query.name+"` ("+ insertColumns +") VALUES " + values

                await newProdDb.execute(queryInsert);

                const progress = Math.min(((start + batchSize) / totalRows) * 100, 100);

                console.log(`Progress for table ${query.name}: ${progress.toFixed(2)}% Index : ${start}`);
                
              }
              catch (error) 
              {
                  if(error.code === "PROTOCOL_CONNECTION_LOST")
                  {
                    console.log("try connect");
                    prodDb = await mysql.createPool(prod);
                    newProdDb = await mysql.createPool(newProd);
                    start = start - batchSize
                    
                  }else{
                    console.log({error , stopIndex : start});
                    break;
                  }
              }

          } 

          console.log(`migration into table ${query.name} : ${start} / ${totalRows}`)
        
      }
    
  } 
  catch (error) 
  {
    console.log("ERROR_CONNECTION_POOL: " + error);
  }

  console.log("END !");
  
}

async function mergitionCms()
{
  try 
  {
    let prod = {
      host    : "localhost",//process.env.DATABASE_CRM_HOST,
      database: "",
      user    : "",
      password: "",
      connectionLimit: 5,
      port: 3314
  }

    let newProd = {
      host: 'localhost',
      user: 'root',
      password: '',
      database: 'xplay_cms'
    }

    let prodDb =  await mysql.createPool(prod);
    let newProdDb =  await mysql.createPool(newProd);

    let selectQueriesCrm = GenerateSelectQueriesCms();

    const batchSize = 1

    let start = 0

      for(let query of selectQueriesCrm.queries)
      {
        // if (query.name != "order_product"){  // you can select table
        //   continue
        // }

        const [result] = await prodDb.execute(query.query + ` LIMIT 0, 1`);
        const totalRows = result[0].rowCount;
        const qr = query.query.replace(", COUNT(*) AS rowCount" , "");

            for (start = 0; start < totalRows; start += batchSize) 
            {

              try 
              {
                const [rows] = await prodDb.execute(qr + ` LIMIT ${start}, ${batchSize}`);

                let insertColumns = Object.keys(rows[0])

                const values = rows.map(row => '(' + insertColumns.map(column => newProdDb.escape(row[column])).join(',') + ')').join(',');

                const queryInsert = "INSERT INTO `"+query.name+"` ("+ insertColumns +") VALUES " + values

                await newProdDb.execute(queryInsert);

                const progress = Math.min(((start + batchSize) / totalRows) * 100, 100);

                console.log(`Progress for table ${query.name}: ${progress.toFixed(2)}% Index : ${start}`);
                
              }
              catch (error) 
              {
                  if(error.code === "PROTOCOL_CONNECTION_LOST")
                  {
                    console.log("try connect");
                    prodDb = await mysql.createPool(prod);
                    newProdDb = await mysql.createPool(newProd);
                    start = start - batchSize
                    
                  }else{
                    console.log({error , stopIndex : start});
                    break;
                  }
              }

          } 

          console.log(`migration into table ${query.name} : ${start} / ${totalRows}`)
        
      }
    
  } 
  catch (error) 
  {
    console.log("ERROR_CONNECTION_POOL: " + error);
  }

  console.log("END !");
  
}

async function mergitionBi()
{
  try 
  {
    let prod = {
      host    : "localhost",//process.env.DATABASE_CRM_HOST,
      database: "xplay_bi",
      user    : "",
      password: "",
      connectionLimit: 5,
      port: 3332
  }

    let newProd = {
      host: 'localhost',
      user: 'root',
      password: '00998877',
      database: 'xplay_bi'
    }

    let prodDb =  await mysql.createPool(prod);
    let newProdDb =  await mysql.createPool(newProd);

    let selectQueriesCrm = GenerateSelectQueriesBi();

    const batchSize = 10000

    let start = 0

      for(let query of selectQueriesCrm.queries)
      {
        // if (query.name != "order_product"){  // you can select table
        //   continue
        // }

        const [result] = await prodDb.execute(query.query + ` LIMIT 0, 1`);
        const totalRows = result[0].rowCount;
        const qr = query.query.replace(", COUNT(*) AS rowCount" , "");

            for (start = 0; start < totalRows; start += batchSize) 
            {

              try 
              {
                const [rows] = await prodDb.execute(qr + ` LIMIT ${start}, ${batchSize}`);

                let insertColumns = Object.keys(rows[0])

                const values = rows.map(row => '(' + insertColumns.map(column => newProdDb.escape(row[column])).join(',') + ')').join(',');

                const queryInsert = "INSERT INTO `"+query.name+"` ("+ insertColumns +") VALUES " + values

                await newProdDb.execute(queryInsert);

                const progress = Math.min(((start + batchSize) / totalRows) * 100, 100);

                console.log(`Progress for table ${query.name}: ${progress.toFixed(2)}% Index : ${start}`);
                
              }
              catch (error) 
              {
                  if(error.code === "PROTOCOL_CONNECTION_LOST")
                  {
                    console.log("try connect");
                    prodDb = await mysql.createPool(prod);
                    newProdDb = await mysql.createPool(newProd);
                    start = start - batchSize
                    
                  }else{
                    console.log({error , stopIndex : start});
                    break;
                  }
              }

          } 

          console.log(`migration into table ${query.name} : ${start} / ${totalRows}`)
        
      }
    
  } 
  catch (error) 
  {
    console.log("ERROR_CONNECTION_POOL: " + error);
  }

  console.log("END !");
  
}

async function mergitionComm()
{
  try 
  {
    let prod = {
      host    : "localhost",//process.env.DATABASE_CRM_HOST,
      database: "xplay_comm",
      user    : "",
      password: "",
      connectionLimit: 5,
      port: 3342
  }

    let newProd = {
      host: 'localhost',
      user: 'root',
      password: '',
      database: 'xplay_comm'
    }

    let prodDb =  await mysql.createPool(prod);
    let newProdDb =  await mysql.createPool(newProd);

    let selectQueriesCrm = GenerateSelectQueriesComm();

    const batchSize = 10000

    let start = 0

      for(let query of selectQueriesCrm.queries)
      {
        // if (query.name != "order_product"){  // you can select table
        //   continue
        // }

        const [result] = await prodDb.execute(query.query + ` LIMIT 0, 1`);
        const totalRows = result[0].rowCount;
        const qr = query.query.replace(", COUNT(*) AS rowCount" , "");

            for (start = 0; start < totalRows; start += batchSize) 
            {

              try 
              {
                const [rows] = await prodDb.execute(qr + ` LIMIT ${start}, ${batchSize}`);

                let insertColumns = Object.keys(rows[0])

                const values = rows.map(row => '(' + insertColumns.map(column => newProdDb.escape(row[column])).join(',') + ')').join(',');

                const queryInsert = "INSERT INTO `"+query.name+"` ("+ insertColumns +") VALUES " + values

                await newProdDb.execute(queryInsert);

                const progress = Math.min(((start + batchSize) / totalRows) * 100, 100);

                console.log(`Progress for table ${query.name}: ${progress.toFixed(2)}% Index : ${start}`);
                
              }
              catch (error) 
              {
                  if(error.code === "PROTOCOL_CONNECTION_LOST")
                  {
                    console.log("try connect");
                    prodDb = await mysql.createPool(prod);
                    newProdDb = await mysql.createPool(newProd);
                    start = start - batchSize
                    
                  }else{
                    console.log({error , stopIndex : start});
                    break;
                  }
              }

          } 

          console.log(`migration into table ${query.name} : ${start} / ${totalRows}`)
        
      }
    
  } 
  catch (error) 
  {
    console.log("ERROR_CONNECTION_POOL: " + error);
  }

  console.log("END !");
  
}



function GenerateSelectQueriesCrm()
{
    const tableNames = [
        "ban_reason", // ok
        "country", // ok
        "day", // ok
        "delete_reason", // ok
        "endpoint", // ok
        "gender", // ok
        "social_network", // ok
        "user", // ok , edit length username + email + country_code , mobile
        "user_address", //ok edit length country_code
        "user_advertisement", // ok
        "user_ban_reason", // ok
        "user_blacklist", // ok
        "user_block", //ok
        "user_delete_reason", // ok
        "user_device", // ok
        "user_featured", // ok
        "user_following", // ok
        "user_forbiden_usernames", //ok
        "user_hash_reset_password", // ok
        "user_hash_verify_email", //ok
        "user_login_attempts", //ok
        "user_mute", //ok
        "user_otp", // ok edit burnt_on to allow null form database
        "user_point", // ok
        "user_preference", // ok , edit length
        "user_preferred_console", // ok
        "user_preferred_game", // ok
        "user_preferred_genre", // ok
        "user_twitter_outh", // ok
        "subscription_vendor", // ok
        "subscription", // ok
        "subscription_recharge_schedule", // ok
        "subscription_transaction_history", // ok
        "user_social_provider",
        "user_social_network" // ok
    ];

    let toReturn = [];

    for(let tableName of tableNames)
    {
        let query = `SELECT * , COUNT(*) AS rowCount FROM ${tableName} `;

        if (tableName == "user_ban_reason" || tableName == "user_blacklist" || tableName == "user_hash_reset_password" || tableName == "user_hash_verify_email" || tableName == "user_preferred_game" || tableName == "user_preferred_genre" || tableName == "user_preferred_notification_event" || tableName == "user_social_network" )
        {
            query = "SELECT tb1.* , COUNT(*) AS rowCount FROM "+ tableName +" tb1 LEFT JOIN user u on tb1.`user_id` = u.`user_id` WHERE u.`user_id` IS NOT NULL "
        }
        else if (tableName == "user_login_attempts")
        (
            query = "SELECT `user_id` , `login_attempts` As attempts, `last_login_attempts_at` As last_attempt , COUNT(*) AS rowCount FROM `user` "
        )
        else if (tableName == "user_point")
        {
            query = "SELECT u.`user_id`, tp1.`total_points` AS points, tp1.`posted_at` AS created_at , COUNT(*) AS rowCount FROM `user_point` tp1  LEFT JOIN `user` u ON tp1.`user_id` = u.`user_id`  WHERE u.`user_id` IS NOT NULL "
        }
        else if (tableName == "user_preference")
        {
            query = "SELECT `user_id`,`is_matchmaking_enabled`,`is_2FA_required`,`preferred_currency`,`preferred_language`,`preferred_theme`,`preferred_timezone` , COUNT(*) AS rowCount FROM `user` "
        }
        else if(tableName == "user_social_provider")
        {
            query = "SELECT `user_id`,`refrence_provider_id` ,`reference_refresh_token`, COUNT(*) AS rowCount FROM `user` WHERE `provider` != 'stcplay'"
        }
        else if (tableName == "user_twitter_outh") 
        {
            query = "SELECT * , COUNT(*) AS rowCount FROM `user_twitter_outh_token` "
        }
        else if (tableName == "user") 
        {
            query = "SELECT `user_id`, `reference_wallet_id`, `email`, `mobile`, `mobile_code`, `username`, `password`, `display_name`, `bio`, `total_followers`, `total_following`, `gender_code`, `nationality_code`, `country_code`, `avatar_url`, `birthdate`, `referred_by`, `regesteration_persona`, `registration_endpoint_code`, `has_whatsapp_access`, `is_password_update_required`, `is_admin`, `is_email_verified`, `is_mobile_verified`, `is_official_account`, `created_at`, `is_legacy_user`, `is_profile_complete`, `is_active`, `is_deleted`, `updated_at`, `referral_code`, `provider`, `is_username_update_required`, `is_banned` , COUNT(*) AS rowCount FROM `user`"
        }
        else if (tableName == "user_address") // ok
        {
          query = "SELECT user_address_id, user_id, name, country_code, mobile, address, label, city, country, zip_code, location_long, location_lat,created_at,updated_at , COUNT(*) AS rowCount FROM user_address"
        }
        else if (tableName == "user_otp") // ok ,  edit burnt_on to allow null form database
        {
          query = "SELECT uo.otp_id, uo.user_id , uo.reference_entity_id, uo.otp, uo.type, uo.otp_attempt, uo.is_burnt, uo.burnt_on, uo.created_on , COUNT(*) AS rowCount FROM user_otp uo "
        }
        else if (tableName == "user_login_attempts") 
        {
          query = "SELECT user_id , login_attempts AS `attempts` , last_login_attempts_at AS `last_attempt` , COUNT(*) AS rowCount FROM `user`"
        }
        else if (tableName == "user_advertisement") // ok
        {
          query = "SELECT tp1.user_advertisement_id ,tp1.user_id, tp1.advertisement_id,tp1.is_deleted,tp1.created_at ,tp1.updated_at , COUNT(*) AS rowCount  FROM user_advertisement tp1"
        }
        else if (tableName == "user_device") // ok
        {
          query = "SELECT ud.* , COUNT(*) AS rowCount FROM user_device ud left join user u ON ud.user_id = u.user_id WHERE u.user_id IS NOT NULL"
        }


        toReturn.push({ name: tableName, query: query });

    }

    return {queries : toReturn, tables: tableNames }
}

function GenerateSelectQueriesFms()
{
    const tableNames = [
        'wallet_type', // ok
        'currency', // ok
        'gateway', // ok
        'refrence_type', // ok
        'country_currency', // ok
        'wallet', // updated_at allow null , last_deduction_at allow null
        'transaction_type', // ok
        'transaction_status', // ok
        'transaction', // ok
        'transaction_history', // ok
        'stcpay_auth_history', // ok
        'tabby_history' // ok
    ];

    let toReturn = [];

    for(let tableName of tableNames)
    {
        let query = `SELECT * , COUNT(*) AS rowCount FROM ${tableName} `;
        
        toReturn.push({ name: tableName, query: query });
    }

    return {queries : toReturn, tables: tableNames };
}

function GenerateSelectQueriesOms()
{
    const tableNames = [
        'vendor', // ok
        'user_return_reason', // ok
        'fulfillment_option_type', // ok
        'fulfilment_method_type', // ok
        'order_status', // ok
        'payment_method_type', // ok
        'return_reason',
        'return_reject_reason',
        'return_status',
        'return_type',
        'address',
        'lmd',
        'order_status',
        'order',
        'preorder',
        'shipment_status',
        'shipment_digital',
        'shipment_physical',
        'order_history',
        'order_product',
        'physical_product_serial',
        'return',
        'return_product',
        'return_product_media',
        'shipment_physical_product',
    ];

    let toReturn = [];

    for(let tableName of tableNames)
    {
        let query = `SELECT * , COUNT(*) AS rowCount FROM ${tableName} `;

        if (tableName == "address")
        {
            query = "SELECT order_id, billing_name, billing_email, billing_mobile, billing_address, billing_postal_code, billing_longitude, billing_latitude, billing_city, billing_country_code, shipping_name, shipping_email, shipping_mobile, shipping_address, shipping_postal_code, shipping_longitude, shipping_latitude, shipping_city, shipping_country_code, fulfilment_method_code, fulfilment_digital_email, fulfilment_digital_mobile, fulfilment_digital_country_code , COUNT(*) AS rowCount FROM `order` "
        }
        else if (tableName == "order")
        {
            query = "SELECT order_id, reference_user_id, lmd_code, invoice_number, lang, status_code, payment_method_code, cancel_reason_code, is_paid, is_checkout, updated_at, created_at, amount_total, amount_sub_total, amount_discount, amount_tax, amount_total as total_amount , amount_grand_total , COUNT(*) AS rowCount FROM `order` "
        }
        else if (tableName == "shipment_digital")
        {
            query = "SELECT shipment_digital_id, order_id, order_product_id, shipment_name, sent_to, reference_receipt_number, reference_serial_number, reference_serial_id, reference_serial_value, reference_expiration_date, fulfilment_method_code, shipment_status_code, created_at , COUNT(*) AS rowCount FROM `shipment_digital` "
        }
        else if (tableName == "return_product")
        {
            query = "SELECT return_product_id, return_id, order_id, order_product_id, reference_product_serial_id as product_serial_id, amount, return_type, refund_method, status_code, return_reason_code, is_accepted, is_refunded, is_active, is_canceled, reference_shipment_id, reject_reason_ar, reject_reason_en, created_at, updated_at , COUNT(*) AS rowCount FROM `return_product` "
        }
        else if (tableName == "return")
        {
            query = "SELECT return_id, return_number, order_id, type, reference_user_id, status_code,  created_at, updated_at, COUNT(*) AS rowCount FROM `return` "
        }else if (tableName == "order_product")
        {
            // query = "SELECT order_product_id, order_id, reference_product_id as product_id,reference_organization_code AS store_code, reference_combination_id AS variant_id, store_code, reference_product_name as `name`, EAN as barcode, vendor_code as supplier, sku, qty, unit_price as actual_cost_unit_price, subtotal as actual_cost_sub_total, total_tax as actual_cost_total_tax, grand_total as actual_cost_grand_total, unit_price as selling_cost_unit_price, subtotal as selling_cost_sub_total, total_tax as selling_cost_total_tax, grand_total as selling_cost_grand_total, discount_price, is_digital, is_deleted , COUNT(*) AS rowCount FROM `order_product` "
            query = "SELECT order_product_id, order_id, reference_product_id as product_id,reference_organization_code AS store_code, reference_combination_id AS variant_id, store_code, reference_product_name as `name`, EAN as barcode, vendor_code as supplier, sku, qty, unit_price as actual_cost_unit_price, subtotal as actual_cost_sub_total, total_tax as actual_cost_total_tax, grand_total as actual_cost_grand_total, unit_price as selling_cost_unit_price, subtotal as selling_cost_sub_total, total_tax as selling_cost_total_tax, grand_total as selling_cost_grand_total, discount_price, is_digital, is_deleted , COUNT(*) AS rowCount FROM order_product op WHERE (order_id, reference_combination_id, reference_product_id, order_product_id) IN ( SELECT order_id, reference_combination_id, reference_product_id, MAX(order_product_id) AS max_order_product_id FROM order_product GROUP BY order_id, reference_combination_id, reference_product_id)"
        }
        else if (tableName == "lmd")
        {
            query = "SELECT lmd_id, region, city, branch_code, erp_store_status, lmd_subinventory, lmd_enabled, virtual_lmd_mapping, store_code, store_name, store_address, store_latitude, store_longitude, is_deleted , COUNT(*) AS rowCount FROM `lmd` "
        }
        else if (tableName == "return_status")
        {
          query = "SELECT return_status_id , return_code AS return_status_code , return_status_ar , return_status_en , COUNT(*) AS rowCount FROM `return_status`"
        }
        else if (tableName == "physical_product_serial")
        {
          query = "SELECT physcial_product_serial_id AS physical_product_serial_id, order_id, order_product_id, imei_number, reserved_store_code, move_order_id, collect_order_id, sale_id, item_code, is_reserved, is_collected, is_deleted, created_at, reserved_at, collected_at, updated_at, deleted_at, is_return_requested, tracking_number , COUNT(*) AS rowCount FROM physical_product_serial"
        }
        
        toReturn.push({ name: tableName, query: query });
    }

    return {queries : toReturn, tables: tableNames };
}

function GenerateSelectQueriesShop()
{
    const tableNames = 
    [
      "product", // Column 'product_code' cannot be null , 
      "product_media", // ok
      "variant", // ok
      "variant_media", // ok
      "variant_option", // ok 
      "option_type", // ok
      "product_option_type", // can't because type_id is uniq , not allow in new structure
      "product_option_value", // same related with `product_option_type`
      "product_related" // ok
    ];

    let toReturn = [];

    for(let tableName of tableNames)
    {
        let query = `SELECT * , COUNT(*) AS rowCount FROM ${tableName} `;

        if (tableName == "product")
        {
          query = "SELECT product_id, sku AS product_code, name_ar,name_en,description_ar,description_en,description_en AS metadata,img_url AS featured_img , vendor_code AS supplier , is_digital , highest_price AS highest_actual_cost , highest_price AS highest_selling_price , lowest_price AS lowest_actual_cost ,lowest_price AS lowest_selling_price , is_active, is_deleted,created_at,updated_at , COUNT(*) AS rowCount FROM `product`"
        }
        else if (tableName == "variant")
        {
          query = "SELECT product_attribute_combination_id AS variant_id , product_id, sku, EAN AS barcode, quantity,price AS selling_price , price AS original_cost , price AS unit_price , weight,is_active,is_deleted,created_at,updated_at , COUNT(*) AS rowCount FROM`product_attribute_combination` WHERE is_deleted = 0"
        }
        else if (tableName == "variant_media")
        {
          query = "SELECT tp1.product_attribute_combination_image_id AS variant_media_id, tp2.product_attribute_combination_id AS variant_id, tp1.image AS media_url, tp1.is_deleted, tp1.sort_id , COUNT(*) AS rowCount FROM product_attribute_combination_image tp1 LEFT JOIN product_attribute_combination tp2 ON tp1.product_attribute_combination_id = tp2.product_attribute_combination_id WHERE tp2.is_deleted = 0"
        }
        else if (tableName == "variant_option")
        {
          query = "SELECT tp1.attribute_combination_value_id AS variant_option_id , tp1.product_id , tp1.attribute_combination_id AS variant_id , tp1.product_attribute_value_id AS product_option_value_id , tp1.is_active , tp1.is_deleted , COUNT(*) AS rowCount FROM `product_attribute_combination_value` tp1 LEFT JOIN product_attribute_combination tp2 ON tp1.attribute_combination_id = tp2.product_attribute_combination_id WHERE tp2.is_deleted = 0"
        }
        else if (tableName == "product_option_value")
        {
          query = "SELECT product_attribute_value_id AS `product_option_value_id` , product_id,product_attribute_id AS product_option_type_id , product_attribute_value_ar AS name_ar , product_attribute_value_en AS name_en ,is_deleted,created_at , COUNT(*) AS rowCount FROM `product_attribute_value`"
        }
        else if (tableName == "option_type")
        {
          query = "SELECT product_attribute_type_id AS option_type_id ,attribute_code AS option_type_code, attribute_name_en AS name_en, attribute_name_ar AS name_ar,is_deleted , COUNT(*) AS rowCount FROM product_attribute_type"
        }
        else if (tableName == "product_option_type")
        {
          query = "SELECT tp1.product_attribute_id AS product_option_type_id,tp1.product_id, tp2.product_attribute_type_id AS option_type_id, tp1.is_deleted , COUNT(*) AS rowCount FROM product_attribute tp1 LEFT JOIN product_attribute_type tp2 ON tp1.product_attribute_type_code = tp2.attribute_code GROUP BY tp1.product_id, tp2.product_attribute_type_id"
        }
        else if (tableName == "product_related")
        {
          query = "SELECT product_related_id, product_id, related_product_id AS related_id, is_deleted , COUNT(*) AS rowCount FROM `product_related`"
        }
        else if (tableName == "product_media")
        {
          query = "SELECT product_media_id, product_id, media_url, media_type, is_deleted , COUNT(*) AS rowCount FROM `product_media`"
        }
        
        toReturn.push({ name: tableName, query: query });
    }

    return {queries : toReturn, tables: tableNames };
}

function GenerateSelectQueriesCms()
{
    const tableNames = [
      "flag_type",
       "flag",
       "platform",
       "game_platform",
       "game",
       "gift_card",
       "gift_card",
       "gift_card_status",

    ];

    let toReturn = [];

    for(let tableName of tableNames)
    {
        let query = `SELECT * , COUNT(*) AS rowCount FROM ${tableName} `;
        
        toReturn.push({ name: tableName, query: query });
    }

    return {queries : toReturn, tables: tableNames };
}

function GenerateSelectQueriesBi()
{
  const tableNames = [
    'active30',
    'activities_demographic',
    'activity',
    'activity_types',
    'bi_setting',
    'daily_errors_insights',
    'daily_sms',
    'daily_snapshot',
    'event',
    'recipient_email',
    'recipient_email_order_late',
    'recipient_mobile',
    'revenue_demographic',
    'skip_word',
    'statistic',
    'top_timeline_word',
    'user_activity',
    'user_activity_history'
];

    let toReturn = [];

    for(let tableName of tableNames)
    {
        let query = `SELECT * , COUNT(*) AS rowCount FROM ${tableName} `;

        if (tableName == "active30")
        {
          query = "SELECT active30_id AS active_30_id , date , COUNT(*) AS rowCount FROM "+ tableName
          tableName = "active_30"
        }
        
        toReturn.push({ name: tableName, query: query });
    }

    return {queries : toReturn, tables: tableNames };
}

function GenerateSelectQueriesComm()
{
    const tableNames = [
      "announcement",
      "conversation",
      "conversation_call_history",
      "conversation_call_status",
      "conversation_privacy",
      "conversation_privacy_level",
      "conversation_tournament",
      "conversation_tournament_user",
      "conversation_types",
      "conversation_user"
    ];

    let toReturn = [];

    for(let tableName of tableNames)
    {
        let query = `SELECT * , COUNT(*) AS rowCount FROM ${tableName} `;

        if (tableName == "announcement")
        {
          query = "SELECT announcement_id, title_ar, title_en, content_ar, content_en, url, is_sent, is_deleted,created_at, updated_at, scheduled_for , COUNT(*) AS rowCount FROM announcement "
        }
        
        toReturn.push({ name: tableName, query: query });
    }

    return {queries : toReturn, tables: tableNames };
}






