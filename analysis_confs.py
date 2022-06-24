#####################################################
################ ANALYSIS SETTINGS ##################
#####################################################

analysis_directory = {}

# SETTINGS FOR PATTERN PRE-FILTERING
pattern_filter_description = {}
pattern_filter_cypher = {}

# SETTINGS FOR CLUSTERING
encoding = {}
num_clusters = {}
exclude_clusters = {}

# SETTINGS FOR SUBSET TO ANALYZE
cluster_descriptions = {}

for pattern_subset in ["P_freq_geq_10__C_20"]:
    pattern_filter_description[pattern_subset] = "freq_geq_10"
    pattern_filter_cypher[pattern_subset] = "WHERE frequency > 9"
    encoding[pattern_subset] = [False, "A_OW", False]
    if pattern_subset == "P_freq_geq_10__C_20":
        analysis_directory[pattern_subset] = f"output\\bpic2017_case_attr\\P_freq_geq_10__C_20"
        num_clusters[pattern_subset] = 20
        exclude_clusters[pattern_subset] = [3, 6]
        cluster_descriptions[pattern_subset] = {"start": "start",
                                                "end": "end",
                                                0: "[0]\n(A)Accept, (O)Create,\n(W)Call offers-start, (A)Complete",
                                                1: "[1]\n(A)Create, (A)Concept",
                                                2: "[2]\n(A)Create, (A)Submit\n(W)Handle Lds-start",
                                                3: "",
                                                4: "[4]\n(O)Accept, (A)Pending,\n(W)Call inc/Validate-end",
                                                5: "[5]\n(A)Create, (A)Concept\n(A)Accept, (O)Create\n"
                                                   "(W)Call offers-start, (A)Complete",
                                                6: "",
                                                7: "[7]\n(A)Denied, (O)Refused\n(W)Call inc/Validate-end",
                                                8: "[8]\n(W)Call offers/Call inc-start\n(W)Validate-start, (A)Validating\n"
                                                   "(W)Validate-end, (W)Call inc-start\n(A)Incomplete",
                                                9: "[9]\n(O)Create",
                                                10: "[10]\n(A)Create, (A)Concept\n(A)Accept, (O)Create\n"
                                                    "(W)Call offers-start, (A)Complete\n(W)Validate, (A)Validating\n"
                                                    "(W)Call inc, (A)Incomplete",
                                                11: "[11]\n(W)Validate-end, (W)Call inc-start,\n(A)Incomplete",
                                                12: "[12]\n(A)Cancel, (O)Cancel\n(W)Call inc/Validate-end",
                                                13: "[13]\n(W)Handle Lds-end, (W)Complete appl-start\n(A)Concept",
                                                14: "[14]\n(W)Call offers/Call inc-end\n(W)Validate-start, (A)Validating",
                                                15: "[15]\n(W)Call inc-end, (W)Validate-start\n(A)Validating, (A)Pending\n"
                                                    "(A)Offer, (W)Validate-end",
                                                16: "[16]\n(A)Create, (A)Concept\n(A)Accept, (O)Create",
                                                17: "[17]\n(W)Handle Lds-end, (A)Concept\n(A)Accept, (O)Create\n"
                                                    "(W)Call offers-start, (A)Complete",
                                                18: "[18]\n(A)Accept, (O)Create",
                                                19: "[19]\n(O)Create, (W)Complete appl-end\n(W)Call offers-start, (A)Complete"}
