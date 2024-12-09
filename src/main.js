const { Actor, RequestList, RequestQueue, CheerioCrawler, ProxyConfiguration, log } = require('apify');
const urlParse = require('url-parse');

function makeUrlFull(href, urlParsed) {
    if (href.startsWith('/')) return urlParsed.origin + href;
    return href;
}

function getIdFromUrl(url) {
    const match = url.match(/(?<=jk=).*?$/);
    return match ? match[0] : '';
}

const fromStartUrls = async function* (startUrls, name = 'STARTURLS') {
    const rl = await RequestList.open(name, startUrls);
    let rq;
    while (rq = await rl.fetchNextRequest()) {
        yield rq;
    }
};

Actor.main(async () => {
    const input = await Actor.getInput() || {};
    const {
        country,
        maxConcurrency,
        position,
        location,
        startUrls,
        extendOutputFunction,
        proxyConfiguration = {
            apifyProxyGroups: [],
            apifyProxyCountry: null,
        },
    } = input;

    let { maxItems } = input;

    if (maxItems > 990) {
        log.warning(`The limit of items exceeds the maximum allowed value. Max possible number of offers is 990.`);
    } else if (maxItems === undefined) {
        log.info(`No maxItems value provided. Setting it to 990 (max).`);
        maxItems = 990;
    }

    let extendOutputFunctionValid;
    if (extendOutputFunction) {
        try {
            extendOutputFunctionValid = eval(extendOutputFunction);
        } catch (e) {
            throw new Error(`extendOutputFunction is not valid JavaScript! Error: ${e}`);
        }
        if (typeof extendOutputFunctionValid !== 'function') {
            throw new Error('extendOutputFunction is not a function! Please fix it or use the default output.');
        }
    }

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

    let countryUrl = countryDict[country?.toLowerCase()] || `https://${country || 'www'}.indeed.com`;

    let itemsCounter = 0;
    let currentPageNumber = 1;

    const requestQueue = await RequestQueue.open();

    if (Array.isArray(startUrls) && startUrls.length > 0) {
        for await (const req of fromStartUrls(startUrls)) {
            if (!req.url) throw new Error('StartURL must have a "url" field.');
            req.userData = req.userData || {};
            req.userData.label = req.userData.label || 'START';
            req.userData.currentPageNumber = currentPageNumber;
            if (req.url.includes('viewjob')) req.userData.label = 'DETAIL';
            if (!req.url.includes('&sort=date')) req.url = `${req.url}&sort=date`;
            await requestQueue.addRequest(req);
            log.info(`URL added to queue: ${req.url}`);
            countryUrl = `https://${req.url.split('https://')[1].split('/')[0]}`;
        }
    } else {
        log.info(`Running site crawl for country: ${country}, position: ${position}, location: ${location}`);
        const startUrl = `${countryUrl}/jobs?${position ? `q=${encodeURIComponent(position)}&sort=date` : ''}${location ? `&l=${encodeURIComponent(location)}` : ''}`;
        await requestQueue.addRequest({
            url: startUrl,
            userData: {
                label: 'START',
                currentPageNumber,
            },
        });
    }

    const proxyConfigOptions = {
        groups: proxyConfiguration.apifyProxyGroups || [],
    };

    if (typeof proxyConfiguration.apifyProxyCountry === 'string' && proxyConfiguration.apifyProxyCountry.trim() !== '') {
        proxyConfigOptions.countryCode = proxyConfiguration.apifyProxyCountry;
    }

    const proxyConfig = new ProxyConfiguration(proxyConfigOptions);

    if (Actor.isAtHome() && !proxyConfig) {
        throw new Error('You must use Apify Proxy or custom proxies to run this scraper on the platform!');
    }

    log.info('Starting crawler...');
    const crawler = new CheerioCrawler({
        requestQueue,
        useSessionPool: true,
        sessionPoolOptions: {
            maxPoolSize: 50,
            sessionOptions: {
                maxUsageCount: 50,
            },
        },
        maxConcurrency,
        maxRequestRetries: 5,
        proxyConfiguration: proxyConfig,
        handlePageFunction: async ({ $, request, response }) => {
            log.info(`Processing ${request.url} with label ${request.userData.label}`);
            // Implement your `handlePageFunction` logic here
        },
    });

    await crawler.run();
    log.info('Crawl finished.');
});
