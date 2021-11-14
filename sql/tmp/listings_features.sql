select 
    l.*,  
    pq.*,
    loc.*,
    a.*
from
    `source.listings` l left join
    `tmp.listing_photos_quality` pq on pq.listing_id = l.id left join
    `source.location` loc on l.location_id = loc.id left join
    `tmp.agents` a on l.agent_id = a.id
