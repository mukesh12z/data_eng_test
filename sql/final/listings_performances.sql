select 
    lp.*, lf.*
from 
    `tmp.listings_features` lf left join
    `tmp.listing_performances` lp on lp.listing_id = lf.id
