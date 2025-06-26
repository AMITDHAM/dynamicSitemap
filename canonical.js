import dotenv from 'dotenv';
import { S3Client, PutObjectCommand } from '@aws-sdk/client-s3';
import axios from 'axios';
import { parseStringPromise } from 'xml2js';
import { JSDOM } from 'jsdom';
import fs from 'fs';
import path from 'path';
import sqlite3 from 'sqlite3';
import { open } from 'sqlite';
import ExcelJS from 'exceljs';
import pLimit from 'p-limit';
import nodemailer from 'nodemailer';

dotenv.config({ path: '.env.local' });

const SAVE_DIR = process.cwd();
const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
const OUTPUT_FILE = path.join(SAVE_DIR, `canonical_mismatches_${timestamp}.xlsx`);
const CACHE_DB = path.join(SAVE_DIR, 'url_cache.sqlite');

const { S3_BUCKET, S3_PUBLIC_PATH5, region, accessKeyId, secretAccessKey, EMAIL_USER, EMAIL_PASS } = process.env;

const s3Client = new S3Client({
  region,
  credentials: {
    accessKeyId,
    secretAccessKey,
  },
});

const limit = pLimit(30);

// Add/Remove sitemaps here
const sitemapList = [
  "https://www.jobtrees.com/sitemap_page.xml",
  "https://www.jobtrees.com/sitemap_hierarchy.xml",
  "https://www.jobtrees.com/sitemap_article.xml",
  "https://www.jobtrees.com/sitemap_role.xml",
  "https://www.jobtrees.com/sitemap_tree.xml",
  "https://www.jobtrees.com/sitemap_video.xml",
  "https://www.jobtrees.com/sitemap_videoArticle.xml",
  "https://www.jobtrees.com/api/sitemap_pSEO/sitemap_index_pSEO.xml",
  "https://www.jobtrees.com/api/sitemap_city/sitemap_index_browse_city.xml",
  "https://www.jobtrees.com/api/sitemap_role/sitemap_index_browse_role.xml",
  "https://www.jobtrees.com/api/sitemap/sitemap_Alljobs.xml",
  "https://www.jobtrees.com/api/sitemap/jobtrees_postings_1.xml",
  "https://www.jobtrees.com/api/sitemap_role/sitemap_index_browse_company.xml"
];

const initDb = async () => {
  const db = await open({ filename: CACHE_DB, driver: sqlite3.Database });
  await db.exec(`
    CREATE TABLE IF NOT EXISTS cache (
      url TEXT PRIMARY KEY,
      canonical TEXT,
      status INTEGER
    )
  `);
  return db;
};

const isCached = async (url, db) => {
  return db.get('SELECT canonical, status FROM cache WHERE url = ?', [url]);
};

const updateCache = async (url, canonical, status, db) => {
  await db.run('REPLACE INTO cache (url, canonical, status) VALUES (?, ?, ?)', [url, canonical, status]);
};

const fetchSitemap = async (url, visited = new Set()) => {
  if (visited.has(url)) return [];
  visited.add(url);

  try {
    const res = await axios.get(url);
    const parsed = await parseStringPromise(res.data);
    const urls = [];

    if (parsed.urlset?.url) {
      for (const entry of parsed.urlset.url) {
        const loc = entry.loc[0];
        if (loc.endsWith('.xml')) {
          urls.push(...await fetchSitemap(loc, visited));
        } else {
          urls.push(loc);
        }
      }
    }
    return urls;
  } catch (err) {
    console.error(`Error fetching sitemap: ${url} - ${err.message}`);
    return [];
  }
};

// Measure page load time with Axios
const measurePageLoadTime = async (pageUrl) => {
  const start = Date.now();
  try {
    const response = await axios.get(pageUrl);
    const loadTime = Date.now() - start;
    return loadTime;
  } catch (err) {
    return -1; // If it fails, return a negative value (indicating failure)
  }
};

const fetchCanonicalUrl = async (pageUrl, db) => {
  const startTime = Date.now();
  const cached = await isCached(pageUrl, db);
  if (cached) return { canonicalUrl: cached.canonical, statusCode: cached.status, responseTime: cached.responseTime };

  try {
    const response = await axios.get(pageUrl, { maxRedirects: 10 }); // Allow redirects to be captured
    const responseTime = Date.now() - startTime;

    const dom = new JSDOM(response.data);
    const canonicalTag = dom.window.document.querySelector('link[rel="canonical"]');
    const canonicalUrl = canonicalTag?.href || 'None'; // Always return canonical, even if it's 'None'

    await updateCache(pageUrl, canonicalUrl, response.status, db);

    return { canonicalUrl, statusCode: response.status, responseTime };
  } catch (err) {
    console.error(`Error fetching ${pageUrl}: ${err.message}`);
    const statusCode = err.response?.status || -1;
    await updateCache(pageUrl, 'None', statusCode, db); // Cache 'None' for canonical if there's an error
    return { canonicalUrl: 'None', statusCode, responseTime: -1 };
  }
};

const checkCanonicalMismatch = async (urlList, db) => {
  return Promise.all(urlList.map(url =>
    limit(async () => {
      const { canonicalUrl, statusCode, responseTime } = await fetchCanonicalUrl(url, db);
      const pageLoadTime = await measurePageLoadTime(url);

      const status = canonicalUrl && canonicalUrl !== url ? 'Fail' : 'Pass';
      console.log(`${status}: ${url} → ${canonicalUrl || 'None'}`);

      return {
        url,
        canonicalUrl,
        status,
        statusCode,
        responseTime,
        pageLoadTime
      };
    })
  ));
};

const saveExcelFile = async (sitemapResultsMap) => {
  const workbook = new ExcelJS.Workbook();

  for (const [sheetName, rows] of sitemapResultsMap.entries()) {
    const worksheet = workbook.addWorksheet(sheetName.slice(0, 31)); // Excel sheet name limit = 31 chars

    worksheet.columns = [
      { header: 'URL', key: 'url', width: 50 },
      { header: 'Canonical URL', key: 'canonicalUrl', width: 50 },
      { header: 'Status', key: 'status', width: 10 },
      { header: 'Status Code', key: 'statusCode', width: 15 },
      { header: 'Response Time (ms)', key: 'responseTime', width: 20 },
      { header: 'Page Load Time (ms)', key: 'pageLoadTime', width: 20 },
    ];

    rows.forEach(row => worksheet.addRow(row));
  }

  await workbook.xlsx.writeFile(OUTPUT_FILE);
  console.log(`📁 Excel saved at ${OUTPUT_FILE}`);
};

const uploadToS3 = async (filePath) => {
  try {
    const stream = fs.createReadStream(filePath);
    const Key = `${S3_PUBLIC_PATH5}canonical_mismatches_${timestamp}.xlsx`;

    await s3Client.send(new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key,
      Body: stream,
      ACL: 'public-read',
    }));

    console.log(`✅ Uploaded to S3: ${Key}`);
  } catch (err) {
    console.error('❌ Upload to S3 failed:', err.message);
  }
};

const sendEmailNotification = async (count) => {
  if (!EMAIL_USER || !EMAIL_PASS) return;
  if (count < 100) return;

  const transporter = nodemailer.createTransport({
    service: 'gmail',
    auth: {
      user: EMAIL_USER,
      pass: EMAIL_PASS,
    },
  });

  await transporter.sendMail({
    from: EMAIL_USER,
    to: 'amit@jobtrees.com',
    subject: '⚠️ Too Many Canonical Mismatches',
    text: `More than 100 mismatches found.\nPlease review the attached Excel: canonical_mismatches_${timestamp}.xlsx`,
  });

  console.log('📧 Email sent!');
};

(async () => {
  const db = await initDb();
  const sitemapResultsMap = new Map();
  let totalMismatches = 0;

  for (const sitemap of sitemapList) {
    console.log(`\n🔍 Checking sitemap: ${sitemap}`);
    const urls = await fetchSitemap(sitemap);
    const results = await checkCanonicalMismatch(urls, db);
    sitemapResultsMap.set(path.basename(sitemap), results);
    totalMismatches += results.filter(r => r.status === 'Fail').length;
  }

  await saveExcelFile(sitemapResultsMap);
  await uploadToS3(OUTPUT_FILE);
  await sendEmailNotification(totalMismatches);
  console.log('\n✅ Done checking canonical mismatches!');
})();
