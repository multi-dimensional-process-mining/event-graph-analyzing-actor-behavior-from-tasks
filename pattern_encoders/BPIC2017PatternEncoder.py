import pandas as pd


class BPIC2017PatternEncoder:

    def __init__(self, merge_events, event_priority, use_count):
        self.encoding_description = f"{event_priority}"
        if use_count:
            self.encoding_description = self.encoding_description + "_c"
        if merge_events:
            self.encoding_description = self.encoding_description + "_m"
        self.encoding = [merge_events, use_count, event_priority]
        self.events_to_merge = [
            [["O_Sent (mail and online)+COMPLETE", "O_Sent (online only)+COMPLETE"], "O_Sent+COMPLETE"],
            [["W_Call after offers+ATE_ABORT", "W_Call after offers+WITHDRAW"], "W_Call after offers+END"],
            [["W_Call incomplete files+ATE_ABORT", "W_Call incomplete files+COMPLETE"], "W_Call incomplete files+END"],
            [["W_Complete application+WITHDRAW", "W_Complete application+ATE_ABORT", "W_Complete application+COMPLETE"],
             "W_Complete application+END"],
            [["W_Handle leads+WITHDRAW", "W_Handle leads+ATE_ABORT", "W_Handle leads+COMPLETE"], "W_Handle leads+END"],
            [["W_Validate application+ATE_ABORT", "W_Validate application+COMPLETE"], "W_Validate application+END"]
            ]

    def encode(self, df_task_variants):
        df_task_variants_encoded = pd.get_dummies(pd.DataFrame(df_task_variants['path'].tolist(), index=df_task_variants.index).stack()).sum(level=0)
        if self.encoding[0]:
            for event in self.events_to_merge:
                df_task_variants_encoded = merge_events(df_task_variants_encoded, event)
        if not self.encoding[1]:
            for (column_name, column_data) in df_task_variants_encoded.iteritems():
                df_task_variants_encoded.loc[df_task_variants_encoded[column_name] > 1, column_name] = 1
        if self.encoding[2] != "AOW":
            if self.encoding[2] == "AO_W":
                for column in df_task_variants_encoded:
                    if column[0] != "W":
                        df_task_variants_encoded[column] = df_task_variants_encoded[column] * 2
            elif self.encoding[2] == "A_OW":
                for column in df_task_variants_encoded:
                    if column[0] == "A":
                        df_task_variants_encoded[column] = df_task_variants_encoded[column] * 4
            elif self.encoding[2] == "A_O_W":
                for column in df_task_variants_encoded:
                    if column[0] == "A":
                        df_task_variants_encoded[column] = df_task_variants_encoded[column] * 4
                    elif column[0] == "O":
                        df_task_variants_encoded[column] = df_task_variants_encoded[column] * 2
            return df_task_variants_encoded

    def get_encoding_description(self):
        return self.encoding_description


def merge_events(df_encoded_tasks, event):
    if len(event[0]) == 3:
        df_encoded_tasks[event[1]] = df_encoded_tasks[event[0][0]] + df_encoded_tasks[event[0][1]] + \
                                     df_encoded_tasks[event[0][2]]
        df_encoded_tasks.drop([event[0][0], event[0][1], event[0][2]], axis=1, inplace=True)
    elif len(event[0]) == 2:
        df_encoded_tasks[event[1]] = df_encoded_tasks[event[0][0]] + df_encoded_tasks[event[0][1]]
        df_encoded_tasks.drop([event[0][0], event[0][1]], axis=1, inplace=True)
    return df_encoded_tasks
