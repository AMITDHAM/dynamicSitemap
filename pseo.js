import {
  S3Client,
  PutObjectCommand,
} from '@aws-sdk/client-s3';
import { create } from 'xmlbuilder';
import dotenv from 'dotenv';
import pLimit from 'p-limit';
import aws4 from 'aws4';
import fs from 'fs';

dotenv.config({ path: '.env.local' });

// --------------------- Configuration ---------------------
const limit = pLimit(30); // Throttle concurrent ES requests
const region = process.env.region;
const credentials = {
  accessKeyId: process.env.accessKeyId,
  secretAccessKey: process.env.secretAccessKey,
};
const S3_BUCKET = process.env.S3_BUCKET;
const OPEN_SEARCH_URL = process.env.OPEN_SEARCH_URL;

// Validate required environment variables
const requiredEnvVars = ['region', 'accessKeyId', 'secretAccessKey', 'S3_BUCKET', 'OPEN_SEARCH_URL'];
const missingEnvVars = requiredEnvVars.filter(varName => !process.env[varName]);
if (missingEnvVars.length > 0) {
  console.error('❌ Missing required environment variables:', missingEnvVars);
  process.exit(1);
}

const PATHS = {
  all: process.env.S3_PUBLIC_PATH2,
  city: process.env.S3_PUBLIC_PATH3,
  role: process.env.S3_PUBLIC_PATH4,
  company: process.env.S3_PUBLIC_PATH6,
  remote: process.env.S3_PUBLIC_PATH7 || 'sitemap_remote/',
  companyJobs: process.env.S3_PUBLIC_PATH8 || 'sitemap_company_jobs/',
  companyLocation: process.env.S3_PUBLIC_PATH9 || 'sitemap_company_location/',
  companyRole: process.env.S3_PUBLIC_PATH10 || 'sitemap_company_role/',
  companyRoleLocation: process.env.S3_PUBLIC_PATH11 || 'sitemap_company_role_location/',
};

console.log('🚀 Initializing S3 client with region:', region);
const s3Client = new S3Client({ region, credentials });

// --------------------- Load Data ---------------------
const getLocations = () => {
  console.log('📁 Loading locations data from cities.json');
  try {
    const data = JSON.parse(fs.readFileSync('./cities.json', 'utf-8'));
    const flattened = [];
    for (const [state, cities] of Object.entries(data)) {
      cities.forEach(city => {
        flattened.push({
          city: city.cityName.toLowerCase(),
          state: state.toLowerCase(),
          stateAbbr: city.abbreviation.toLowerCase(),
          zipCodes: city.zipCodes || []
        });
      });
    }
    console.log(`📍 Loaded ${flattened.length} locations`);
    return flattened;
  } catch (error) {
    console.error('❌ Failed to load cities.json:', error.message);
    throw error;
  }
};

const fetchRoles = async () => {
  console.log('📁 Loading roles data from roles.json');
  try {
    const roles = JSON.parse(fs.readFileSync('./roles.json', 'utf-8'));
    console.log(`🎯 Loaded ${roles.length} roles`);
    return roles;
  } catch (error) {
    console.error('❌ Failed to load roles.json:', error.message);
    throw error;
  }
};

const fetchCompanies = async () => {
  console.log('📁 Loading companies data from companies.json');
  try {
    const companies = JSON.parse(fs.readFileSync('./companies.json', 'utf-8'));
    console.log(`🏢 Loaded ${companies.length} companies`);
    return companies;
  } catch (error) {
    console.error('❌ Failed to load companies.json:', error.message);
    throw error;
  }
};

// --------------------- Helpers ---------------------
const formatRole = (r) => r.toLowerCase().replace(/-/g, '--').replace(/ /g, '-');
const formatCity = (c) => c.toLowerCase().replace(/ /g, '-');
const formatCompany = (c) => encodeURIComponent(c.toLowerCase().replace(/ /g, '-'));

const buildUrl = (role, city, stateAbbr) =>
  `https://www.jobtrees.com/browse-careers/${formatRole(role)}-jobs-in-${formatCity(city)}-${stateAbbr}`;

const createXmlContent = (urls) => {
  console.log(`📝 Creating XML content for ${urls.length} URLs`);
  return create('urlset', { version: '1.0', encoding: 'UTF-8' })
    .att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9')
    .ele(
      urls.map(url => ({
        url: {
          loc: url,
          lastmod: new Date().toISOString(),
          changefreq: 'daily',
          priority: '1.0'
        }
      }))
    )
    .end({ pretty: true });
};

const createIndex = (files, type) => {
  console.log(`📑 Creating index for ${files.length} files of type: ${type}`);
  const index = create('urlset', { version: '1.0', encoding: 'UTF-8' })
    .att('xmlns', 'http://www.sitemaps.org/schemas/sitemap/0.9');

  let pathPrefix;
  if (type === 'roleCompany') pathPrefix = PATHS.companyRole;
  else if (type === 'cityCompany') pathPrefix = PATHS.companyLocation;
  else if (type === 'companyOnly') pathPrefix = PATHS.companyJobs;
  else if (type === 'roleCityCompany') pathPrefix = PATHS.companyRoleLocation;
  else pathPrefix = PATHS[type];

  files.forEach(file => {
    index.ele('url')
      .ele('loc', `https://www.jobtrees.com/api/${pathPrefix}${file}`).up()
      .ele('lastmod', new Date().toISOString()).up()
      .ele('changefreq', 'daily').up()
      .ele('priority', '1.0').up();
  });

  return index.end({ pretty: true });
};

const uploadToS3 = async (type, fileName, content) => {
  try {
    let pathPrefix;
    if (type === 'roleCompany') pathPrefix = PATHS.companyRole;
    else if (type === 'cityCompany') pathPrefix = PATHS.companyLocation;
    else if (type === 'companyOnly') pathPrefix = PATHS.companyJobs;
    else if (type === 'roleCityCompany') pathPrefix = PATHS.companyRoleLocation;
    else pathPrefix = PATHS[type];

    const path = `${pathPrefix}${fileName}`;
    console.log(`📤 Uploading to S3: ${path}`);
    await s3Client.send(new PutObjectCommand({
      Bucket: S3_BUCKET,
      Key: path,
      Body: content,
      ContentType: 'application/xml',
      ACL: 'public-read',
    }));
    console.log(`✅ Successfully uploaded: ${path}`);
  } catch (e) {
    console.error(`❌ Upload failed for ${fileName}:`, e.message);
    throw e; // Re-throw to handle in calling function
  }
};

// --------------------- ElasticSearch Job Check ---------------------
const checkJobExists = async (type, role, city, state, company) => {
  const indices = [
    'jobtrees_postings',
    'indeed_jobs_postings',
    'big_job_site_postings',
    'perengo_postings',
    'adzuna_postings',
  ];

  console.log(`🔍 Checking: ${type}, role: ${role || 'N/A'}, city: ${city || 'N/A'}, state: ${state || 'N/A'}, company: ${company || 'N/A'}`);

  const searchRequests = indices.map(async (indexName) => {
    const mustFilters = [
      { exists: { field: "postcode" } },
      { exists: { field: "company" } },
      { exists: { field: "title" } },
      { exists: { field: "state" } },
      { exists: { field: "city" } },
      { term: { "status.keyword": "Active" } }
    ];

    // Add debug logging for the query
    console.log(`   📋 Building query for ${indexName}:`);
    
    if (type === 'all') {
      mustFilters.push({ match: { "jobTreesTitle.keyword": role?.toLowerCase() } });
      mustFilters.push({ term: { "city.keyword": city } });
      mustFilters.push({ term: { "state.keyword": state.trim() } });
      console.log(`     - jobTreesTitle: ${role?.toLowerCase()}`);
      console.log(`     - city: ${city}`);
      console.log(`     - state: ${state.trim()}`);
    }
    if (type === 'role') {
      mustFilters.push({ match: { "jobTreesTitle.keyword": role?.toLowerCase() } });
      console.log(`     - jobTreesTitle: ${role?.toLowerCase()}`);
    }
    if (type === 'city') {
      mustFilters.push({ term: { "city.keyword": city } });
      mustFilters.push({ term: { "state.keyword": state.trim() } });
      console.log(`     - city: ${city}`);
      console.log(`     - state: ${state.trim()}`);
    }
    if (type === 'roleCompany') {
      mustFilters.push({ term: { "company.keyword": company } });
      mustFilters.push({ match: { "jobTreesTitle.keyword": role?.toLowerCase() } });
      console.log(`     - company: ${company}`);
      console.log(`     - jobTreesTitle: ${role?.toLowerCase()}`);
    }
    if (type === 'cityCompany') {
      mustFilters.push({ term: { "company.keyword": company } });
      mustFilters.push({ term: { "city.keyword": city } });
      mustFilters.push({ term: { "state.keyword": state.trim() } });
      console.log(`     - company: ${company}`);
      console.log(`     - city: ${city}`);
      console.log(`     - state: ${state.trim()}`);
    }
    if (type === 'companyOnly') {
      mustFilters.push({ term: { "company.keyword": company } });
      console.log(`     - company: ${company}`);
    }
    if (type === 'roleCityCompany') {
      mustFilters.push({ term: { "company.keyword": company } });
      mustFilters.push({ match: { "jobTreesTitle.keyword": role?.toLowerCase() } });
      mustFilters.push({ term: { "city.keyword": city } });
      mustFilters.push({ term: { "state.keyword": state.trim() } });
      console.log(`     - company: ${company}`);
      console.log(`     - jobTreesTitle: ${role?.toLowerCase()}`);
      console.log(`     - city: ${city}`);
      console.log(`     - state: ${state.trim()}`);
    }

    const searchRequestBody = { query: { bool: { must: mustFilters } }, size: 2 };

    console.log(`   📤 Querying ${indexName}...`);
    
    const searchRequest = {
      host: OPEN_SEARCH_URL,
      path: `/${indexName}/_search`,
      service: 'es',
      region,
      method: 'POST',
      body: JSON.stringify(searchRequestBody),
      headers: { 'Content-Type': 'application/json' },
    };

    aws4.sign(searchRequest, credentials);
    try {
      const searchResponse = await fetch(`https://${searchRequest.host}${searchRequest.path}`, {
        method: searchRequest.method,
        headers: searchRequest.headers,
        body: searchRequest.body,
      });
      
      if (!searchResponse.ok) {
        console.warn(`   ⚠️  Request failed: ${searchResponse.status} ${searchResponse.statusText}`);
        const errorBody = await searchResponse.text();
        console.warn(`   📄 Error response: ${errorBody.substring(0, 200)}...`);
        return false;
      }
      
      const searchResponseBody = await searchResponse.json();
      const jobCount = searchResponseBody.hits?.total?.value || 0;
      
      console.log(`   📊 ${indexName}: Found ${jobCount} jobs`);
      
      if (jobCount > 0 && jobCount <= 5) {
        console.log(`   👀 Sample jobs found:`);
        searchResponseBody.hits.hits.slice(0, 3).forEach((hit, i) => {
          console.log(`     ${i+1}. ${hit._source.title} at ${hit._source.company} in ${hit._source.city}, ${hit._source.state}`);
        });
      }
      
      const hasJobs = jobCount > 1;
      return hasJobs;
      
    } catch (error) {
      console.error(`   ❌ Error querying ${indexName}:`, error.message);
      if (error.code) console.error(`   🔧 Error code: ${error.code}`);
      return false;
    }
  });

  const results = await Promise.all(searchRequests);
  const hasJobs = results.some(result => result);
  
  console.log(`   🎯 Overall result: ${hasJobs ? 'PASS' : 'FAIL'}`);
  console.log(`   ----------------------------------------`);
  
  return hasJobs;
};

const checkJobExistsThrottled = (type, role, city, state, company) =>
  limit(() => checkJobExists(type, role, city, state, company));

// --------------------- Generate Sitemap ---------------------
const generateSitemap = async (type, jobs, roles, locations, companies) => {
  console.log(`\n🌐 Starting sitemap generation for type: ${type}`);
  console.log(`📊 Roles: ${roles.length}, Locations: ${locations.length}, Companies: ${companies.length}`);
  
  const MAX = 5000;
  let urls = [];
  let checkedCount = 0;
  let passedCount = 0;

  if (type === 'role') {
    for (const role of roles) {
      checkedCount++;
      if (await checkJobExistsThrottled(type, role)) {
        urls.push(`https://www.jobtrees.com/browse-careers/${formatRole(role)}`);
        passedCount++;
      }
    }
  }
  else if (type === 'city') {
    for (const loc of locations) {
      checkedCount++;
      if (await checkJobExistsThrottled(type, null, loc.city, loc.state)) {
        urls.push(`https://www.jobtrees.com/browse-careers/${formatCity(loc.city)}-${loc.stateAbbr}`);
        passedCount++;
      }
    }
  }
  else if (type === 'company') {
    // Company type doesn't check ElasticSearch
    for (const loc of locations) {
      urls.push(`https://www.jobtrees.com/top-companies/${formatCity(loc.city)}-${loc.stateAbbr}`);
    }
    urls.push('https://www.jobtrees.com/top-companies/remote-us');
    passedCount = urls.length;
  }
  else if (type === 'all') {
    for (const role of roles) {
      for (const loc of locations) {
        checkedCount++;
        if (await checkJobExistsThrottled('all', role, loc.city, loc.state)) {
          urls.push(buildUrl(role, loc.city, loc.stateAbbr));
          passedCount++;
        }
      }
    }
  }
  else if (type === 'roleCompany') {
    for (const company of companies) {
      for (const role of roles) {
        checkedCount++;
        if (await checkJobExistsThrottled(type, role, null, null, company)) {
          urls.push(`https://www.jobtrees.com/browse-companies/${formatCompany(company)}/${formatRole(role)}-jobs`);
          passedCount++;
        }
      }
    }
  }
  else if (type === 'cityCompany') {
    for (const company of companies) {
      for (const loc of locations) {
        checkedCount++;
        if (await checkJobExistsThrottled(type, null, loc.city, loc.state, company)) {
          urls.push(`https://www.jobtrees.com/browse-companies/${formatCompany(company)}/jobs-in-${formatCity(loc.city)}-${loc.stateAbbr}`);
          passedCount++;
        }
      }
    }
  }
  else if (type === 'companyOnly') {
    for (const company of companies) {
      checkedCount++;
      if (await checkJobExistsThrottled(type, null, null, null, company)) {
        urls.push(`https://www.jobtrees.com/browse-companies/${formatCompany(company)}-jobs`);
        passedCount++;
      }
    }
  }
  else if (type === 'roleCityCompany') {
    for (const company of companies) {
      for (const role of roles) {
        for (const loc of locations) {
          checkedCount++;
          if (await checkJobExistsThrottled(type, role, loc.city, loc.state, company)) {
            urls.push(`https://www.jobtrees.com/browse-companies/${formatCompany(company)}/${formatRole(role)}-jobs-in-${formatCity(loc.city)}-${loc.stateAbbr}`);
            passedCount++;
          }
        }
      }
    }
  }

  console.log(`📈 ${type}: Checked ${checkedCount} combinations, ${passedCount} passed job check`);

  // --------------------- Chunk and Upload ---------------------
  const prefixMap = {
    role: 'sitemap_browse_role',
    city: 'sitemap_browse_city',
    company: 'sitemap_browse_company',
    all: 'pSEO_page',
    roleCompany: 'sitemap_company_role',
    cityCompany: 'sitemap_company_location',
    companyOnly: 'sitemap_company_jobs',
    roleCityCompany: 'sitemap_company_role_location',
  };
  const indexFileMap = {
    role: 'sitemap_index_browse_role.xml',
    city: 'sitemap_index_browse_city.xml',
    company: 'sitemap_index_browse_company.xml',
    all: 'sitemap_index_pSEO.xml',
    roleCompany: 'sitemap_index_company_role.xml',
    cityCompany: 'sitemap_index_company_location.xml',
    companyOnly: 'sitemap_index_company_jobs.xml',
    roleCityCompany: 'sitemap_index_company_role_location.xml',
  };

  if (urls.length === 0) {
    console.log(`⏭️  No URLs generated for type: ${type}, skipping upload`);
    return;
  }

  const chunks = [];
  for (let i = 0; i < urls.length; i += MAX) chunks.push(urls.slice(i, i + MAX));

  console.log(`📦 Splitting into ${chunks.length} chunks for type: ${type}`);

  const sitemapFiles = await Promise.all(chunks.map((chunk, i) =>
    limit(async () => {
      const file = `${prefixMap[type]}_${i + 1}.xml`;
      const xml = createXmlContent(chunk);
      await uploadToS3(type, file, xml);
      return file;
    })
  ));

  const indexXml = createIndex(sitemapFiles, type);
  await uploadToS3(type, indexFileMap[type], indexXml);
  console.log(`✅ ${type} sitemap complete. ${urls.length} URLs included in ${chunks.length} files.`);
};

// --------------------- Main ---------------------
(async () => {
  try {
    console.log('🚀 Starting sitemap generation process');
    console.log('========================================');
    
    const startTime = Date.now();
    
    const [roles, locations, companies] = await Promise.all([
      fetchRoles(),
      getLocations(),
      fetchCompanies()
    ]);

    console.log('========================================');
    console.log('📊 Data loaded successfully:');
    console.log(`   - Roles: ${roles.length}`);
    console.log(`   - Locations: ${locations.length}`);
    console.log(`   - Companies: ${companies.length}`);
    console.log('========================================');

    // Generate sitemaps in sequence to avoid overwhelming resources
    const sitemapTypes = [
      'role',
      'city',
      'company',
      'all',
      'roleCompany',
      'cityCompany',
      'companyOnly',
      'roleCityCompany',
    ];

    for (const type of sitemapTypes) {
      console.log(`\n🎯 Processing sitemap type: ${type}`);
      console.log('----------------------------------------');
      await generateSitemap(type, null, roles, locations, companies);
      console.log('----------------------------------------');
    }

    const endTime = Date.now();
    const duration = ((endTime - startTime) / 1000 / 60).toFixed(2);
    
    console.log('\n🎉 All sitemaps generated successfully!');
    console.log(`⏱️  Total time: ${duration} minutes`);
    console.log('========================================');

  } catch (error) {
    console.error('💥 Fatal error in main process:', error.message);
    console.error(error.stack);
    process.exit(1);
  }
})();