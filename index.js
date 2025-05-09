const express = require('express');
const { logOpenByCid, insertTrackingRow } = require('./google');

// Initialize Express app first before using it
const app = express();

// Read environment variables with validation
const port = process.env.PORT || 3000;

// Validate critical environment variables on startup
if (!process.env.GOOGLE_SERVICE_ACCOUNT) {
  console.error('❌ CRITICAL ERROR: GOOGLE_SERVICE_ACCOUNT environment variable is missing');
  // Continue execution to allow health checks to pass
}

if (!process.env.GOOGLE_SHEET_ID) {
  console.error('❌ CRITICAL ERROR: GOOGLE_SHEET_ID environment variable is missing');
  // Continue execution to allow health checks to pass
}

// Add health check endpoint for Railway
app.get('/health', (req, res) => {
  res.status(200).send('OK');
});

// Handle process signals properly
process.on('SIGTERM', () => {
  console.log('🛑 SIGTERM received, shutting down gracefully');
  // Allow some time for pending requests to complete
  setTimeout(() => {
    process.exit(0);
  }, 1000);
});

process.on('uncaughtException', (err) => {
  console.error('❌ Uncaught exception:', err);
  // Don't exit to allow the server to continue running
});

// ✅ Base64 websafe decoder to exactly match Apps Script encoding
function decodeBase64UrlSafe(str) {
  str = str.replace(/-/g, '+').replace(/_/g, '/');
  while (str.length % 4 !== 0) str += '=';
  const buffer = Buffer.from(str, 'base64');
  return buffer.toString('utf-8');
}

app.get('/open', async (req, res) => {
  try {
    const encodedCid = req.query.cid;
    if (!encodedCid) {
      console.error('❌ Missing cid parameter');
      return res.status(400).send('Missing cid');
    }

    // Log the raw CID for debugging
    console.log('📨 Received raw CID:', encodedCid);

    // Add cache-control headers to prevent caching
    res.set({
      'Cache-Control': 'no-store, no-cache, must-revalidate, proxy-revalidate',
      'Pragma': 'no-cache',
      'Expires': '0',
      'Surrogate-Control': 'no-store'
    });

    // ✅ Decode only for log (not for matching)
    try {
      const decoded = decodeBase64UrlSafe(encodedCid);
      console.log('📨 Decoded CID:', decoded);
      
      // Handle both separator formats (| and ||)
      let parts;
      if (decoded.includes('||')) {
        parts = decoded.split('||');
      } else {
        parts = decoded.split('|');
      }
      
      const [company, email, subject, type, sentTime] = parts;
      console.log('📨 Open Pixel Triggered:', { company, email, subject, type, sentTime });

      // Process the tracking event asynchronously
      logOpenByCid(encodedCid.trim()).catch(err => {
        console.error('❌ Error in tracking handler:', err);
      });
      
      // ✅ Return 1x1 pixel immediately to keep email clients happy
      const pixel = Buffer.from('R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==', 'base64');
      res.set('Content-Type', 'image/gif');
      res.send(pixel);
    } catch (decodeErr) {
      console.error('❌ Error decoding CID:', decodeErr, 'Raw CID:', encodedCid);
      // Still return a pixel to avoid broken images in emails
      const pixel = Buffer.from('R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==', 'base64');
      res.set('Content-Type', 'image/gif');
      res.send(pixel);
    }
  } catch (err) {
    console.error('❌ Error processing /open:', err.message, err.stack);
    // Return a pixel anyway to avoid broken images in emails
    const pixel = Buffer.from('R0lGODlhAQABAIAAAAAAAP///ywAAAAAAQABAAACAUwAOw==', 'base64');
    res.set('Content-Type', 'image/gif');
    res.send(pixel);
  }
});

// For testing the connection to Google Sheets
app.get('/status', async (req, res) => {
  try {
    // Check if required environment variables are set
    if (!process.env.GOOGLE_SERVICE_ACCOUNT) {
      return res.status(500).json({ 
        status: 'error', 
        message: 'GOOGLE_SERVICE_ACCOUNT environment variable is missing' 
      });
    }

    if (!process.env.GOOGLE_SHEET_ID) {
      return res.status(500).json({ 
        status: 'error', 
        message: 'GOOGLE_SHEET_ID environment variable is missing' 
      });
    }

    // Check if we can parse service account JSON
    try {
      JSON.parse(process.env.GOOGLE_SERVICE_ACCOUNT);
    } catch (err) {
      return res.status(500).json({ 
        status: 'error', 
        message: 'GOOGLE_SERVICE_ACCOUNT is not valid JSON' 
      });
    }

    res.status(200).json({ 
      status: 'ok', 
      message: 'Environment variables look good', 
      sheetId: process.env.GOOGLE_SHEET_ID,
      serviceAccountPresent: true
    });
  } catch (err) {
    res.status(500).json({ 
      status: 'error', 
      message: err.message 
    });
  }
});

// Start the server
app.listen(port, () => {
  console.log(`🚀 Server running on port ${port}`);
});