const { GoogleSpreadsheet } = require('google-spreadsheet');
const SHEET_NAME = 'Email Tracking Log';

// ✅ Utility to decode web-safe base64 (Google-style)
function decodeBase64UrlSafe(str) {
  str = str.replace(/-/g, '+').replace(/_/g, '/');
  while (str.length % 4 !== 0) str += '=';
  const buffer = Buffer.from(str, 'base64');
  return buffer.toString('utf-8');
}

// Function to safely get credentials
function getServiceAccountCreds() {
  try {
    if (!process.env.GOOGLE_SERVICE_ACCOUNT) {
      console.error('❌ GOOGLE_SERVICE_ACCOUNT environment variable is missing');
      return null;
    }
    return JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
  } catch (err) {
    console.error('❌ Error parsing GOOGLE_SERVICE_ACCOUNT:', err.message);
    console.error('First 100 chars of GOOGLE_SERVICE_ACCOUNT:', 
      process.env.GOOGLE_SERVICE_ACCOUNT ? process.env.GOOGLE_SERVICE_ACCOUNT.substring(0, 100) + '...' : 'undefined');
    return null;
  }
}

// Function to get sheet ID
function getSheetId() {
  const sheetId = process.env.GOOGLE_SHEET_ID;
  if (!sheetId) {
    console.error('❌ GOOGLE_SHEET_ID environment variable is missing');
  }
  return sheetId;
}

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
    const target = rows.find(r => (r['CID'] || '').trim() === trimmedCid);

    if (!target) {
      console.error('❌ CID not found in sheet:', trimmedCid);
      console.log('🔍 First few CIDs in sheet for comparison:');
      if (rows.length > 0) {
        for (let i = 0; i < Math.min(5, rows.length); i++) {
          console.log(`CID ${i+1}:`, (rows[i]['CID'] || '').trim());
        }
      }
      
      // Attempt to create a new row if the CID doesn't exist
      try {
        console.log('⚠️ Attempting to create a new tracking row for the missing CID');
        await insertTrackingRow(company, email, subject, type, new Date(parseInt(sentTime)), trimmedCid);
        
        // Retry finding the row
        const updatedRows = await sheet.getRows();
        const newTarget = updatedRows.find(r => (r['CID'] || '').trim() === trimmedCid);
        
        if (newTarget) {
          console.log('✅ Successfully created and found new tracking row');
          updateOpenStats(newTarget, now);
          await newTarget.save();
          return;
        } else {
          console.error('❌ Failed to create new tracking row');
          return;
        }
      } catch (err) {
        console.error('❌ Error creating new row:', err.message);
        return;
      }
    }

    console.log('✅ Found matching CID in sheet');
    updateOpenStats(target, now);
    await target.save();
    console.log(`✅ Open logged for CID: ${encodedCid}`);
  } catch (err) {
    console.error('❌ Error in logOpenByCid:', err.message);
    console.error(err.stack);
  }
}

// Helper function to update open statistics
function updateOpenStats(row, timestamp) {
  const total = parseInt(row['Total Opens'] || '0') || 0;
  row['Total Opens'] = total + 1;
  row['Last Seen Time'] = timestamp;

  for (let i = 1; i <= 10; i++) {
    const col = `Seen ${i}`;
    if (!row[col]) {
      row[col] = timestamp;
      break;
    }
    if (i === 10) {
      row[col] = timestamp;
    }
  }
}

// 📝 Insert a new tracking row when email is sent
async function insertTrackingRow(company, email, subject, type, sentTime, cid) {
  try {
    console.log('📝 Starting insertTrackingRow for:', { company, email, subject, type });
    
    // Validate environment variables
    const creds = getServiceAccountCreds();
    const SHEET_ID = getSheetId();
    
    if (!creds || !SHEET_ID) {
      console.error('❌ Missing credentials or sheet ID');
      return;
    }

    const doc = new GoogleSpreadsheet(SHEET_ID);
    await doc.useServiceAccountAuth(creds);
    await doc.loadInfo();

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

    // Format date to string if it's a Date object
    const sentTimeStr = sentTime instanceof Date ? sentTime.toISOString() : sentTime.toString();

    await sheet.addRow({
      'Company Name': company,
      'Email ID': email,
      'Subject': subject,
      'Email Type': type,
      'Sent Time': sentTimeStr,
      'Total Opens': '0',
      'Last Seen Time': '',
      'Seen 1': '', 'Seen 2': '', 'Seen 3': '', 'Seen 4': '', 'Seen 5': '',
      'Seen 6': '', 'Seen 7': '', 'Seen 8': '', 'Seen 9': '', 'Seen 10': '',
      'Total PDF Views': '0', 'Last PDF View Time': '',
      'Total Cal Clicks': '0', 'Last Cal Click Time': '',
      'Total Web Clicks': '0', 'Last Web Click Time': '',
      'Total Portfolio Link Clicks': '0', 'Last Portfolio Link Time': '',
      'CID': cid
    });

    console.log(`✅ Row inserted for CID: ${cid}`);
  } catch (err) {
    console.error('❌ Error in insertTrackingRow:', err.message);
    console.error(err.stack);
  }
}

module.exports = {
  logOpenByCid,
  insertTrackingRow
};
