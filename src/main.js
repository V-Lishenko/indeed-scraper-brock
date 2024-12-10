const requestHandler = async ({ $, request, session, response, crawler }) => {
    const { log } = require('apify');
    const { maxItems, extendOutputFunctionValid } = crawler.userData; // Access custom properties from the crawler
    const requestQueue = crawler.requestQueue; // Access the shared request queue
    const urlParsed = urlParse(request.url); // Parse the current URL

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
                    uniqueKey: itemId, // Use unique job key
                    userData: { label: 'DETAIL' },
                });
            });

            for (const detailRequest of details) {
                // Skip invalid or duplicate URLs
                if (
                    itemsCounter >= maxItems ||
                    detailRequest.url.includes('undefined') ||
                    itemsCounter >= 990
                ) {
                    continue;
                }
                await requestQueue.addRequest(detailRequest, { forefront: true });
            }

            // Handle pagination
            const maxItemsOnSite = Number(
                $('#searchCountPages').text().trim().split(' ')[3]?.replace(/[^0-9]/g, '') || 0
            );

            const hasNextPage = $(`a[aria-label="${currentPageNumber + 1}"]`).length > 0;
            if (
                hasNextPage &&
                itemsCounter < maxItems &&
                itemsCounter < 990 &&
                itemsCounter < maxItemsOnSite
            ) {
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
            if (itemsCounter >= maxItems) {
                return;
            }

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

            if (result.postedAt.includes('If you require alternative methods of application or screening')) {
                await Actor.setValue('HTML', $('html').html(), { contentType: 'text/html' });
            }

            if (extendOutputFunctionValid) {
                try {
                    const userResult = await extendOutputFunctionValid($);
                    Object.assign(result, userResult);
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
