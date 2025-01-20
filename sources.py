base_url = 'https://graph.facebook.com/v21.0/'

comments = { # comentarios dado un objeto
        # Doesn't support since and until
        'name': 'comments'
        ,'platform': 'facebook'
        ,'url': base_url
        ,'token_type': 'Page'
        ,'payload': lambda params: f"{params['object_id']}/comments?fields=id,created_time,like_count,comment_count,message,from,message_tags,reactions,application,user_likes,can_remove,permalink_url"
        ,'limit': 100
        ,'filekey': lambda params: params['object_id']
    }

comment_responses = { # comentarios dado un objeto
        # Doesn't support since and until
        'name': 'comment_responses'
        ,'platform': 'facebook'
        ,'url': base_url
        ,'token_type': 'Page'
        ,'payload': lambda params: f"{params['parent_comment_id']}?fields=id,comments{{id,created_time,like_count,comment_count,message,from,message_tags,reactions,application,user_likes,can_remove,permalink_url}}"
        ,'limit': 100
        ,'filekey': lambda params: params['parent_comment_id']
    }

ads_creatives = { # obten los ads y todos los creatives asociados al ad
        # Supports since and until
        'name': 'ads_creatives'
        ,'url': base_url
        ,'token_type': 'Page'
        ,'since':'since'
        ,'until':'until'
        ,'payload': lambda params: f"{params['account_id']}/ads?fields=id,name,account_id,campaign{{id,name}},adset_id,creative,adcreatives{{title,status,source_instagram_media_id,product_set_id,object_story_id,object_id,name,call_to_action_type,object_type,instagram_user_id,instagram_story_id,instagram_actor_id,id,object_store_url,link_url,effective_object_story_id,effective_instagram_story_id,body,actor_id,video_id,account_id}},created_time,effective_status,source_ad_id,status,updated_time,recommendations"
        ,'limit': 100
        ,'filekey': lambda params: params["account_id"]
    }

feed = { # publicaciones del feed
        # Supports since and until
        'name': 'feed'
        ,'url': base_url
        # Para este url conviene usar token de acceso de usuario para evitar limitación de frecuencia
        # pero para mostrar información de usuario en las respuestas sí se requiere
        ,'token_type': 'Page'
        ,'payload': lambda params: f"{params['page_id']}/feed?fields=id,actions{{name}},call_to_action,created_time,feed_targeting,from{{name}},instagram_eligibility,is_eligible_for_promotion,is_expired,is_hidden,is_instagram_eligible,is_popular,is_published,message,message_tags,parent_id,permalink_url,promotable_id,properties,shares,story,story_tags,targeting,to,updated_time"
        ,'limit': 100
        ,'filekey': lambda params: f"{params['page_id']}"
    }

feed_attachments = { # publicaciones del feed con attachments
        # Supports since and until
        'name': 'feed_text_attributes'
        ,'url': base_url
        # Para este url conviene usar token de acceso de usuario para evitar limitación de frecuencia
        # pero para mostrar información de usuario en las respuestas sí se requiere
        ,'token_type': 'Page'
        ,'since': 'since'
        ,'until': 'until'
        ,'payload': lambda params: f"{params['page_id']}/feed?fields=id,actions{{name}},call_to_action,created_time,feed_targeting,from{{name}},instagram_eligibility,is_eligible_for_promotion,is_expired,is_hidden,is_instagram_eligible,is_popular,is_published,message,message_tags,parent_id,permalink_url,promotable_id,properties,shares,story,story_tags,targeting,to,attachments{{title,media_type,target}},updated_time"
        ,'limit': 100
        ,'filekey': lambda params: params['page_id']
    }

feed_posts_message_tags = { # personas etiquetadas en publicaciones
        'name': 'feed_posts_message_tags'
        ,'platform': 'facebook'
        ,'url': base_url
        # Para este url conviene usar token de acceso de usuario para evitar limitación de frecuencia
        # pero para mostrar información de usuario en las respuestas sí se requiere
        ,'token_type': 'Page'
        ,'since': 'since'
        ,'until': 'until'
        ,'payload': lambda params: f"{params['page_id']}/feed?fields=id,message_tags"
        ,'limit': 100
        ,'filekey': lambda params: params['page_id']
    }


posts_text = { # comentarios dado un objeto
        'name': 'posts_text'
        ,'platform': 'facebook'
        ,'url': base_url
        ,'token_type': 'Page'
        ,'payload': lambda params: f"{params['object_id']}?"
        ,'limit': 100
        ,'filekey': lambda params: params['object_id']
    }



campaign_ads = { # obten los ads y todos los creatives asociados al ad
        'name': 'campaign_ads'
        ,'platform': 'facebook'
        ,'url': base_url
        ,'token_type': 'Page'
        ,'since':'since'
        ,'until':'until'
        ,'payload': lambda params: f"{params['campaign_id']}/ads?fields=id,name,account_id,campaign{{id,name}},adset_id,creative,adcreatives{{title,status,source_instagram_media_id,product_set_id,object_story_id,object_id,name,call_to_action_type,object_type,instagram_user_id,instagram_story_id,instagram_actor_id,id,object_store_url,link_url,effective_object_story_id,effective_instagram_story_id,body,actor_id,video_id,account_id}},created_time,effective_status,source_ad_id,status,updated_time,recommendations"
        ,'limit': 100
        ,'filekey': lambda params: params['campaign_id']
    }


campaigns = { # obten los ads y todos los creatives asociados al ad
        'name': 'campaigns'
        ,'platform': 'facebook'
        ,'url': base_url
        ,'token_type': 'Page'
        ,'payload': lambda params: f"{params['account_id']}/campaigns?fields=id,name,objective,created_time,account_id,adlabels,brand_lift_studies,daily_budget,lifetime_budget,effective_status,status,start_time,stop_time,updated_time"
        ,'limit': 100
        ,'filekey': lambda params: params['account_id']
    }

ig_media = { # obten la media publicada en ig
        'name': 'ig_media'
        ,'platform': 'instagram'
        ,'url': base_url
        ,'token_type': 'Page'
        ,'payload': lambda params: f"{params['user_id']}/media?fields=boost_eligibility_info,caption,comments_count,id,ig_id,is_comment_enabled,is_shared_to_feed,legacy_instagram_media_id,like_count,media_product_type,media_type,media_url,owner,permalink,shortcode,thumbnail_url,timestamp,username,boost_ads_list,branded_content_partner_promote,children,collaborators,comments,product_tags"
        ,'limit': 100
        ,'filekey': lambda params: params['user_id']
    }


ig_comments = { # obten los comentarios sobre la media ṕublicada en ig
        'name': 'ig_comments'
        ,'platform': 'instagram'
        ,'url': base_url
        ,'token_type': 'Page'
        ,'payload': lambda params: f"{params['ig_media_id']}/comments?fields=from,hidden,id,legacy_instagram_comment_id,like_count,media,text,timestamp,parent_id,user,username,replies"
        ,'limit': 100
        ,'filekey': lambda params: params['ig_media_id']
    }


ig_comment_replies = { # obten respuestas a comentarios
        'name': 'ig_comment_replies'
        ,'platform': 'instagram'
        ,'url': base_url
        ,'token_type': 'Page'
        ,'payload': lambda params: f"{params['parent_comment_id']}/replies?fields=from,hidden,id,legacy_instagram_comment_id,like_count,media,text,timestamp,parent_id,user,username"
        ,'limit': 100
        ,'filekey': lambda params: params['parent_comment_id']
    }
