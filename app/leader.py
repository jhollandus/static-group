"""
Cycle:

On start, perform join without member ID
    key is consumer group id
    body includes 
        - correlation id

Wait for join-response
    key is consumer group id
    body includes 
        - correlation id
        - member id



"""