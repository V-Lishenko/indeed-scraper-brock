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
            throw new Error(`Blocked by the target on ${request.url}`);
        }

        switch (request.userData.label) {
            case 'START':
            case 'LIST': {
                const noResultsFlag = $('.no_results').length > 0;
                if (noResultsFlag) {
                    log.info(`No results found for URL: ${request.url}`);
                    return;
                }

                const currentPageNumber = request.userData.currentPageNumber || 1;
                const urlDomainBase = new URL(request.url).hostname;

                // Extract job details
                const details = [];
                $('.tapItem a[data-jk]').each((index, element) => {
                    const itemId = $(element).attr('data-jk');
                    const itemUrl = `https://${urlDomainBase}${$(element).attr('href')}`;
                    details.push({
                        url: itemUrl,
                        uniqueKey: itemId,
                        userData: { label: 'DETAIL' },
                    });
                });

                for (const detailRequest of details) {
                    if (itemsCounter >= maxItems) break;
                    await requestQueue.addRequest(detailRequest, { forefront: true });
                }

                // Handle pagination
                const maxItemsOnSite = Number(
                    $('#searchCountPages').text().trim().split(' ')[3]?.replace(/[^0-9]/g, '') || 0
                );

                const hasNextPage = $(`a[aria-label="${currentPageNumber + 1}"]`).length > 0;
                if (hasNextPage && itemsCounter < maxItems && itemsCounter < maxItemsOnSite) {
                    const nextPageUrl = $(`a[aria-label="${currentPageNumber + 1}"]`).attr('href');
                    for (let i = 0; i < 5; i++) {
                        await requestQueue.addRequest({
                            url: makeUrlFull(nextPageUrl, urlParsed),
                            uniqueKey: `${i}-${makeUrlFull(nextPageUrl, urlParsed)}`,
                            userData: { label: 'LIST', currentPageNumber: currentPageNumber + 1 },
                        });
                    }
                }

                break;
            }

            case 'DETAIL': {
                if (itemsCounter >= maxItems) return;

                const result = {
                    positionName: $('.jobsearch-JobInfoHeader-title').text().trim(),
                    salary: $('#salaryInfoAndJobType .attribute_snippet').text() || null,
                    company: $('meta[property="og:description"]').attr('content'),
                    location: $(".css-1tlxeot > div").text(),
                    rating: Number($('meta[itemprop="ratingValue"]').attr('content')) || null,
                    reviewsCount: Number($('meta[itemprop="ratingCount"]').attr('content')) || null,
                    url: request.url,
                    id: getIdFromUrl($('meta[id="indeed-share-url"]').attr('content')),
                    postedAt: $('.jobsearch-JobMetadataFooter>div').not('[class]').text().trim(),
                    scrapedAt: new Date().toISOString(),
                    description: $('div[id="jobDescriptionText"]').text(),
                    externalApplyLink: $('#applyButtonLinkContainer a')?.attr('href') || null,
                };

                if (extendOutputFunctionValid) {
                    try {
                        const extendedData = await extendOutputFunctionValid($);
                        Object.assign(result, extendedData);
                    } catch (error) {
                        log.error(`Error in extendOutputFunction: ${error.message}`);
                    }
                }

                await Actor.pushData(result);
                itemsCounter++;
                break;
            }

            default:
                throw new Error(`Unknown label: ${request.userData.label}`);
        }
    };

    // Initialize and run the crawler
    const crawler = new CheerioCrawler({
        requestQueue,
        proxyConfiguration: proxyConfig,
        maxConcurrency,
        maxRequestRetries: 5,
        requestHandler,
        failedRequestHandler: async ({ request }) => {
            console.error(`Request ${request.url} failed too many times.`);
        },
    });

    console.log('Starting crawler...');
    await crawler.run();
    console.log('Crawl finished.');
});
