class ApiMethods:
    
    """
    This class is the beginning of a more comprehensive collection of methods
    to be used to communicate with the API in a structured way. Thus far, 
    it only contains sending and cancelling orders. More to come...
    """
    
    def __init__(self):
        
        pass
    
    
    
    def send_order(self, instrument=None, side=None, amount=None, 
                   order_type=None, label=None, price=None, time_in_force=None, 
                   max_show=None, post_only=None, reject_post_only=None, 
                   reduce_only=None, trigger=None, trigger_price=None, 
                   trigger_offset=None, advanced=None, mmp=None, valid_until=None:
        
        if instrument == None or side == None or amount == None:
            print("Required arguments not provided: instrument / side / amount")
            message = None
            calltype = None
        
        else:
            calltype = "private/{}".format(side)
            message = dict()
            message["instrument_name"] = instrument
            if amount is not None:
                message["amount"] = amount
            if order_type is not None:
                message["type"] = order_type
            if label is not None:
                message["label"] = label
            if price is not None:
                message["price"] = price
            if time_in_force is not None:
                message["time_in_force"] = time_in_force
            if max_show is not None:
                message["max_show"] = max_show
            if post_only is not None:
                message["post_only"] = post_only
            if reject_post_only is not None:
                message["reject_post_only"] = reject_post_only
            if reduce_only is not None:
                message["reduce_only"] = reduce_only
            if trigger is not None:
                message["trigger"] = trigger                   
            if trigger_price is not None:
                message["trigger_price"] = trigger_price
            if trigger_offset is not None:
                message["trigger_offset"] = trigger_offset
            if advanced is not None:
                message["advanced"] = advanced
            if mmp is not None:
                message["mmp"] = mmp
            if valid_until is not None:
                message["valid_until"] = valid_until
        
        return [message, calltype]
    
    
    def cancel_all(self):
        calltype = "private/cancel_all"
        message = {}
        return [message, calltype]
    
    
    def cancel_last(self, label, currency):
        calltype = "private/cancel_by_label"
        message = {"label":label, "currency":currency}
        return [message, calltype]
    
    
    
    
