select 
    listing_id, 
    average(p.quality) as avg_qual, 
    min(p.quality) as min_qual, 
    max(p.quality) as max_qual
from `source.listing_photos`
group by listing_id

