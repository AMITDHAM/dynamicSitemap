import aws4 from 'aws4';
import dotenv from 'dotenv';
import fs from 'fs';

dotenv.config({ path: '.env.local' });

const region = process.env.region;
const credentials = {
  accessKeyId: process.env.accessKeyId,
  secretAccessKey: process.env.secretAccessKey,
};
const OPEN_SEARCH_URL = process.env.OPEN_SEARCH_URL;

const fetchCompanyIds = async () => {
  const indexName = 'company_suggestion_index';
  const pageSize = 1000;
  let allIds = [];

  try {
    // Initial search request with scroll
    const request = {
      host: OPEN_SEARCH_URL,
      path: `/${indexName}/_search?scroll=1m`,
      service: 'es',
      region,
      method: 'POST',
      body: JSON.stringify({
        size: pageSize,
        _source: false,
        query: { match_all: {} },
      }),
      headers: { 'Content-Type': 'application/json' },
    };

    aws4.sign(request, credentials);

    let response = await fetch(`https://${request.host}${request.path}`, {
      method: request.method,
      headers: request.headers,
      body: request.body,
    });
    let data = await response.json();

    let scrollId = data._scroll_id;
    let hits = data.hits.hits;

    while (hits.length > 0) {
      allIds.push(...hits.map(doc => doc._id));

      // Scroll request
      const scrollReq = {
        host: OPEN_SEARCH_URL,
        path: `/_search/scroll`,
        service: 'es',
        region,
        method: 'POST',
        body: JSON.stringify({
          scroll: '1m',
          scroll_id: scrollId,
        }),
        headers: { 'Content-Type': 'application/json' },
      };

      aws4.sign(scrollReq, credentials);

      response = await fetch(`https://${scrollReq.host}${scrollReq.path}`, {
        method: scrollReq.method,
        headers: scrollReq.headers,
        body: scrollReq.body,
      });
      data = await response.json();

      scrollId = data._scroll_id;
      hits = data.hits.hits || [];
    }

    // Save to file
    fs.writeFileSync('./company_ids.json', JSON.stringify(allIds, null, 2));
    console.log(`✅ Saved ${allIds.length} company IDs to company_ids.json`);
  } catch (error) {
    console.error('❌ Error fetching IDs:', error.message);
  }
};

(async () => {
  await fetchCompanyIds();
})();
