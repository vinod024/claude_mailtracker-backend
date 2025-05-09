// 🔍 Log email open event by CID
async function logOpenByCid(encodedCid) {
  try {
    console.log('🔍 Starting logOpenByCid with CID:', encodedCid);
    
    // Validate environment variables
    const creds = getServiceAccountCreds();
    const SHEET_ID = getSheetId();
    
    if (!creds || !SHEET_ID) {
      console.error('❌ Missing credentials or sheet ID, cannot log open event');
      return;
    }
    
    // Decode CID and extract components
    const decodedCid = decodeBase64UrlSafe(encodedCid);
    console.log('🔍 Decoded CID:', decodedCid);
    
    // Handle both separator formats (| and ||)
    let parts;
    if (decodedCid.includes('||')) {
      parts = decodedCid.split('||');
    } else {
      parts = decodedCid.split('|'); 
    }
    
    const [company, email, subject, type, sentTime] = parts;
    const now = new Date().toLocaleString('en-GB', { timeZone: 'Asia/Kolkata' });

    // Skip logging if the open is from specific email addresses
    if (email && (email.includes('vinodk@tatsa.tech') || email.includes('test@') || email.includes('example.com'))) {
      console.log('⛔ Skipping self-open for sender:', email);
      return;
    }

    console.log('🔍 Connecting to Google Sheets');
    const doc = new GoogleSpreadsheet(SHEET_ID);
    await doc.useServiceAccountAuth(creds);
    
    // Load document with retry logic
    let retries = 3;
    while (retries > 0) {
      try {
        await doc.loadInfo();
        break;
      } catch (err) {
        retries--;
        if (retries === 0) throw err;
        console.warn(`⚠️ Error loading document, retrying (${retries} attempts left):`, err.message);
        await new Promise(resolve => setTimeout(resolve, 1000)); // Wait 1 second before retry
      }
    }
    
    console.log('✅ Successfully connected to Google Sheets');

    // Get the tracking log sheet
    let sheet;
    try {
      sheet = doc.sheetsByTitle[SHEET_NAME];
      if (!sheet) {
        // Try case-insensitive match as fallback
        const sheetTitles = Object.keys(doc.sheetsByTitle);
        const matchingTitle = sheetTitles.find(
          title => title.toLowerCase() === SHEET_NAME.toLowerCase()
        );
        
        if (matchingTitle) {
          sheet = doc.sheetsByTitle[matchingTitle];
          console.log(`Found sheet with slightly different name: "${matchingTitle}"`);
        } else {
          console.error(`❌ Sheet "${SHEET_NAME}" not found. Available sheets:`, sheetTitles);
          return;
        }
      }
    } catch (err) {
      console.error(`❌ Error accessing sheet "${SHEET_NAME}":`, err.message);
      return;
    }

    await sheet.loadHeaderRow();
    
    // Confirm required columns exist
    const requiredColumns = ['CID', 'Total Opens', 'Last Seen Time'];
    const missingColumns = requiredColumns.filter(col => !sheet.headerValues.includes(col));
    
    if (missingColumns.length > 0) {
      console.error(`❌ Required columns missing: ${missingColumns.join(', ')}`);
      console.log('Available columns:', sheet.headerValues);
      return;
    }
    
    const rows = await sheet.getRows();
    console.log(`✅ Loaded ${rows.length} rows from sheet`);

    const trimmedCid = encodedCid.trim();
    
    // First, try to find exact CID match
    let target = rows.find(r => (r['CID'] || '').trim() === trimmedCid);
    
    // If exact match fails, try matching by company, email, subject, and type
    if (!target) {
      console.log('⚠️ Exact CID match not found, trying to match by email details');
      target = rows.find(r => 
        r['Company Name'] === company && 
        r['Email ID'] === email && 
        r['Subject'] === subject &&
        r['Email Type'] === type
      );
    }

    if (!target) {
      console.error('❌ Could not find matching row for tracking');
      console.log('Details:', { company, email, subject, type });
      return;
    }

    console.log('✅ Found matching row in sheet');
    
    // Prevent duplicate tracking within short time periods (5 seconds)
    const lastSeenTime = target['Last Seen Time'];
    if (lastSeenTime) {
      const lastSeen = new Date(lastSeenTime);
      const now = new Date();
      const timeDiff = Math.abs(now - lastSeen);
      
      // If last seen was less than 5 seconds ago, likely a duplicate
      if (timeDiff < 5000) {
        console.log('⚠️ Ignoring probable duplicate open (within 5 seconds)');
        return;
      }
    }
    
    // Update the tracking data
    updateOpenStats(target, now.toLocaleString('en-GB', { timeZone: 'Asia/Kolkata' }));
    await target.save();
    console.log(`✅ Open logged successfully for: ${company}, ${email}, ${subject}`);
  } catch (err) {
    console.error('❌ Error in logOpenByCid:', err.message);
    console.error(err.stack);
  }
}