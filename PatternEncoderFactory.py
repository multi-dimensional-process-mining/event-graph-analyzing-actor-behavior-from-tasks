from pattern_encoders.BPIC2017PatternEncoder import BPIC2017PatternEncoder


def get_pattern_encoder(name_data_set, merge_events=False, event_priority="A_OW", use_count=False):

    if name_data_set == "bpic2017":
        return BPIC2017PatternEncoder(merge_events, event_priority, use_count)
