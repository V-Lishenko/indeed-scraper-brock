const { Actor } = require('apify');
const { CheerioCrawler, ProxyConfiguration } = require('crawlee');
const urlParse = require('url-parse');

/**
 * Generates a full URL from a relative path.
 */
function makeUrlFull(href, urlParsed) {
    return href.startsWith('/') ? urlParsed.origin + href : href;
}

/**
 * Extracts the job ID from a URL.
 */
function getIdFromUrl(url) {
    const match = url.match(/(?<=jk=).*?$/);
    return match ? match[0] : '';
}

/**
 * Helper function to generate requests from start URLs.
 */
const generateRequestsFromStartUrls = async function* (startUrls, name = 'STARTURLS') {
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
        maxConcurrency = 10,
        position,
        location,
        startUrls = [],
        extendOutputFunction,
        maxItems: rawMaxItems,
    } = input;

    // Validate maxItems
    const maxItems = rawMaxItems > 990 ? 990 : rawMaxItems || 990;
    if (rawMaxItems > 990) {
        console.warn('The maximum number of items exceeds 990. Limiting to 990.');
    } else if (!rawMaxItems) {
        console.log('No maxItems value provided. Defaulting to 990.');
    }

    // Validate extendOutputFunction
    let extendOutputFunctionValid;
    if (extendOutputFunction) {
        try {
            extendOutputFunctionValid = eval(extendOutputFunction);
            if (typeof extendOutputFunctionValid !== 'function') {
                throw new Error('extendOutputFunction must be a valid JavaScript function.');
            }
        } catch (error) {
            throw new Error(`Invalid extendOutputFunction: ${error.message}`);
        }
    }

    // Country-specific URLs
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

    // Add start URLs to the request queue
    if (startUrls.length > 0) {
        for await (const req of generateRequestsFromStartUrls(startUrls)) {
            if (!req.url) throw new Error('StartURL must include a "url" field.');
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

    // Configure custom proxy
    const proxyConfig = new ProxyConfiguration({
        proxyUrls: ['http://spzxaz671f:W05Vpv_9ulx7RuwmiF@gate.smartproxy.com:10004'],
    });
    console.log('Using proxy:', proxyConfig.proxyUrls[0]);

    // Define the request handler
    const requestHandler = async ({ $, request, session, response, crawler }) => {
        const { log } = require('apify');
        const urlParsed = urlParse(request.url);

        log.info(`Processing request: ${request.url} | Label: ${request.userData.label}`);

        // Handle non-success HTTP responses
        if (![200, 404, 407].includes(response.statusCode)) {
            session.retire(); // Retire session to avoid reusing the same blocked session
            request.retryCount--; // Decrease retry count to allow retries
            throw new Error(`Blocked with ${response.statusCode} on ${request.url}`);
        }

        // Handle different page types
        switch (request.userData.label) {
            case 'START':
            case 'LIST':
                // Handle list page logic
                break;

            case 'DETAIL':
                // Handle detail page logic
                break;

            default:
                throw new Error(`Unknown label: ${request.userData.label}`);
        }
    };

    // Initialize and run the crawler
    const crawler = new CheerioCrawler({
        requestQueue,
        proxyConfiguration: proxyConfig,
        maxConcurrency,
        maxRequestRetries: 3,
        preNavigationHooks: [
            async ({ request }) => {
                // Add browser-like headers
                request.headers['User-Agent'] =
                    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/111.0.0.0 Safari/537.36';
                request.headers['Accept-Language'] = 'en-US,en;q=0.9';
                request.headers['Referer'] = 'https://www.google.com/';
                request.headers['Accept'] =
                    'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8';
                request.headers['Connection'] = 'keep-alive';
                request.headers['Upgrade-Insecure-Requests'] = '1';

                // Add random delay
                const delay = Math.random() * 5000; // Random delay between 0 and 5 seconds
                await new Promise((resolve) => setTimeout(resolve, delay));
            },
        ],
        requestHandler,
        failedRequestHandler: async ({ request }) => {
            console.error(`Request ${request.url} failed: ${request.errorMessages.join(', ')}`);
        },
    });

    console.log('Starting crawler...');
    await crawler.run();
    console.log('Crawl finished.');
});
