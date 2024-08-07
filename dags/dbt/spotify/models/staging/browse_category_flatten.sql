SELECT 
categories,
categories:href AS CATEGORIES_HREF,
CATEGORIES_ITEMS.VALUE::string AS CATEGORIES_ITEMS,
CATEGORIES_ITEMS.VALUE:href::string AS CATEGORIES_ITEMS_HREF,
CATEGORIES_ITEMS.VALUE:id::string AS CATEGORIES_ITEMS_ID,
CATEGORIES_ITEMS_ICONS.VALUE::string AS CATEGORIES_ITEMS_ICONS,
CATEGORIES_ITEMS_ICONS.VALUE:height::string AS CATEGORIES_ITEMS_ICONS_HEIGHT,
CATEGORIES_ITEMS_ICONS.VALUE:url::string AS CATEGORIES_ITEMS_ICONS_URL,
CATEGORIES_ITEMS_ICONS.VALUE:width::string AS CATEGORIES_ITEMS_ICONS_WIDTH,
CATEGORIES_ITEMS.VALUE:name::string AS CATEGORIES_ITEMS_NAME,
categories:limit AS CATEGORIES_LIMIT,
categories:next AS CATEGORIES_NEXT,
categories:offset AS CATEGORIES_OFFSET,
categories:previous AS CATEGORIES_PREVIOUS,
categories:total AS CATEGORIES_TOTAL
FROM {{source('spotify', 'browse_category')}},
LATERAL FLATTEN(input => CATEGORIES:items) CATEGORIES_ITEMS,
LATERAL FLATTEN(input => CATEGORIES_ITEMS.value:icons) CATEGORIES_ITEMS_ICONS