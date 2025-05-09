app.get('/open', async (req, res) => {
  try {
    const encodedCid = req.query.cid;
    if (!encodedCid) {
      console.error('❌ Missing cid parameter');
      return res.status(400).send('Missing cid');
    }

    // Log the raw CID for debugging
    console.log('📨 Received raw CID:', encodedCid);

    // Add cache-control headers to prevent caching and reduce duplicate tracking
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