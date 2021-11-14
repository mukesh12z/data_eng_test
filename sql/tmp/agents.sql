select
    a.*,
    ag.name,
    s.name
from 
    `source.agents` a left join
    `source.agency` ag on a.agency_id = ag.id left join
    `source.subscriptions` s on a.subscription_id = s.id

