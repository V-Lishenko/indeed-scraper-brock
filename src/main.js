const { Actor } = require('apify');
const { CheerioCrawler } = require('crawlee');
const urlParse = require('url-parse');

/**
 * Create full URL from relative paths.
 */
function makeUrlFull(href, urlParsed) {
    return href.startsWith('/') ? urlParsed.origin + href : href;
}

/**
 * Extract job ID from URL.
 */
function getIdFromUrl(url) {
    const match = url.match(/(?<=jk=).*?$/);
    return match ? match[0] : '';
}

/**
 * Helper function to generate requests from start URLs.
 */
const fromStartUrls = async function* (startUrls, name = 'STARTURLS') {
    const requestList = await Actor.openRequestList(name, startUrls);
    let request;
    while ((request = await requestList.fetchNextRequest())) {
        yield request;
    }
};

Actor.main(async () => {
    const input = await Actor.getInput() || {};
    const {
        country,
        maxConcurrency = 10, // Default to 10 if not provided
        position,
        location,
        startUrls = [],
        extendOutputFunction,
        proxyConfiguration, // Ignored because we're using a custom proxy
    } = input;

    let { maxItems } = input;

    // Validate and adjust maxItems
    if (maxItems > 990) {
        console.warn('The limit of items exceeds the maximum allowed value. Limiting to 990.');
        maxItems = 990;
    } else if (maxItems === undefined) {
        console.log('No maxItems value provided. Setting it to 990.');
        maxItems = 990;
    }

    // Validate extendOutputFunction
    let extendOutputFunctionValid;
    if (extendOutputFunction) {
        try {
            extendOutputFunctionValid = eval(extendOutputFunction);
            if (typeof extendOutputFunctionValid !== 'function') {
                throw new Error('extendOutputFunction is not a valid function.');
            }
        } catch (error) {
            throw new Error(`Invalid extendOutputFunction: ${error.message}`);
        }
    }

    // Country base URLs
    const countryDict = {
        us: 'https://www.indeed.com',
        uk: 'https://www.indeed.co.uk',
        gb: 'https://www.indeed.co.uk',
        fr: 'https://www.indeed.fr',
        es: 'https://www.indeed.es',
        in: 'https://www.indeed.co.in',
        br: 'https://www.indeed.com.br',
        ca: 'https://www.indeed.ca',
        nl: 'https://www.indeed.nl',
        za: 'https://www.indeed.co.za',
    };

    const countryUrl = countryDict[country?.toLowerCase()] || `https://${country || 'www'}.indeed.com`;

    let itemsCounter = 0;
    const requestQueue = await Actor.openRequestQueue();

    // Process start URLs
    if (startUrls.length > 0) {
        for await (const req of fromStartUrls(startUrls)) {
            if (!req.url) throw new Error('StartURL must have a "url" field.');
            req.userData = req.userData || { label: 'START', currentPageNumber: 1 };
            if (req.url.includes('viewjob')) req.userData.label = 'DETAIL';
            if (!req.url.includes('&sort=date')) req.url = `${req.url}&sort=date`;
            await requestQueue.addRequest(req);
            console.log(`Added URL to queue: ${req.url}`);
        }
    } else {
        console.log(`Generating initial URL for country: ${country}, position: ${position}, location: ${location}`);
        const startUrl = `${countryUrl}/jobs?${position ? `q=${encodeURIComponent(position)}&sort=date` : ''}${location ? `&l=${encodeURIComponent(location)}` : ''}`;
        await requestQueue.addRequest({
            url: startUrl,
            userData: { label: 'START', currentPageNumber: 1 },
        });
    }

    // Configure the custom proxy
    const proxyUrl = 'http://spzxaz671f:W05Vpv_9ulx7RuwmiF@gate.smartproxy.com:10004';
    console.log('Using proxy:', proxyUrl);

    // Initialize and run the crawler
    console.log('Starting crawler...');
    const crawler = new CheerioCrawler({
        requestQueue,
        maxConcurrency,
        maxRequestRetries: 5,
        preNavigationHooks: [
            async ({ request, session }) => {
                // Attach proxy URL to each request
                session.setProxyUrl(proxyUrl);
                request.headers['User-Agent'] = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36';
            },
        ],
        handlePageFunction: async ({ $, request, response }) => {
            console.log(`Processing ${request.url} with label ${request.userData.label}`);
            // Implement your scraping logic here
        },
        failedRequestHandler: async ({ request }) => {
            console.error(`Request ${request.url} failed too many times.`);
        },
    });

    await crawler.run();
    console.log('Crawl finished.');
});
