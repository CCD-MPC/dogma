import json
import copy

from conclave.dag import *

from dogma.net import setup_peer


class Verify:

    def __init__(self, protocol, policy: [str, dict], conf):

        self.protocol = OpDag(protocol())
        self.config = conf
        self.policy = self.setup_policy(policy)
        self.pid = conf["user_config"]["pid"]
        self.peer = None

    @staticmethod
    def setup_policy(p: [str, dict]):

        if isinstance(p, dict):
            return p
        elif isinstance(p, str):
            with open(p, 'r') as pol:
                return json.load(pol)
        else:
            raise Exception("TypeError: Policy must be either dict or JSON.\n")

    def setup_networked_peer(self):

        self.peer = setup_peer(self.config, self.policy)

        return self

    def _find_root(self, policy):
        """
        For a given policy, find it's corresponding root node.
        """

        node_name = policy["fileName"]
        root_nodes = self.protocol.roots

        for rn in root_nodes:
            if rn.out_rel.name == node_name:
                return rn

        raise Exception("Node {} not found in DAG.\n".format(node_name))

    def _handle_aggregate(self, column, node):
        """
        Determine if column in the set {agg_col, group_cols}.
        Update idx and name accordingly.
        """

        if node.agg_col.name == column.name:
            new_col = node.out_rel.columns[-1]
            column.name = new_col.name
            column.idx = new_col.idx
            return column.verify()

        elif column.name in [n.name for n in node.group_cols]:
            for n in node.group_cols:
                if n.name == column.name:
                    column.idx = n.idx

            return self._continue_traversal(column, node)

        else:
            # column isn't present in output, can be verified.
            return column.verify()

    def _handle_concat(self, column, node):
        """
        Concat relations can rename columns, so update
        column name by its idx in the output relation.
        """

        # can assume that all columns present in input
        # are also present in the output.
        col_name = node.out_rel.columns[column.idx].name
        column.name = col_name

        return self._continue_traversal(column, node)

    def _handle_project(self, column, node):
        """
        Project relations can involve shuffling of columns
        (but not renaming), so update column idx by name.
        """

        for c in node.out_rel.columns:
            if c.name == column.name:
                column.idx = c.idx
                return self._continue_traversal(column, node)

        """
        NOTE - we don't automatically verify the column here because it is possible
        to perform some backwards inferrable operation on a revealable column
        (e.g. - multiply) from a non-revealable column and then project out the
        non-revealable column from the relation. Thus, even though this column is
        not explicitly part of the output, we still treat it as such to avoid this kind
        of exploit.
        """
        return self._continue_traversal(column, node)

    @staticmethod
    def _rewrite_column_for_left(column, node):
        """
        Update column idx according to it's idx in the
        join node's output relation.
        """

        num_cols_in_left = len(node.left_parent.out_rel.columns)

        for i in range(num_cols_in_left):
            if column.name == node.out_rel.columns[i].name:
                column.idx = node.out_rel.columns[i].idx
                return column

        raise Exception("Column from left wasn't present in Join output relation.\n")

    @staticmethod
    def _rewrite_column_for_right(column, node):
        """
        Determine where this column is in the output relation
        and overwrite it's name / idx as needed.
        """

        right_join_cols = [n.name for n in node.right_join_cols]
        right_non_join_cols = [c.name for c in node.right_parent.out_rel.columns if c.name not in right_join_cols]

        if column.name in right_join_cols:
            for i in range(len(right_join_cols)):
                if right_join_cols[i].name == column.name:
                    # join col names from left rel overwrite
                    # right col names in output relation.
                    column.name = node.out_rel.columns[i].name
                    column.idx = i
                    return column

        elif column.name in right_non_join_cols:
            for i in range(len(right_join_cols), len(node.out_rel.columns)):
                if node.out_rel.columns[i].name == column.name:
                    column.idx = i
                    return column

        else:
            raise Exception("Column from right wasn't present in Join output relation")

    def _handle_join(self, column, node):
        """
        Map column name / idx from appropriate column in output rel to this column.
        """

        left_parent_name = node.left_parent.out_rel.name
        right_parent_name = node.right_parent.out_rel.name

        if left_parent_name == column.current_rel_name:
            column = self._rewrite_column_for_left(column, node)

        elif right_parent_name == column.current_rel_name:
            column = self._rewrite_column_for_right(column, node)

        else:
            raise Exception("Current node not present in parent relations.\n")

        return self._continue_traversal(column, node)

    def _continue_traversal(self, column, node):
        """
        Continue traversing the DAG. Only handling cases where the node
        is either terminal or has exactly one child for now.
        """

        column.update_rel_name(node)

        if len(node.children) == 1:
            return self._verify_column(column, node.children.pop())
        elif len(node.children) == 0:
            return column
        else:
            raise NotImplementedError("Workflows with node splitting not yet implemented.\n")

    def _verify_column(self, column, node):
        """
        For a given column, traverse the DAG and determine if the
        workflow is compatible with it's policy.
        """

        # if column's policy states that it can be revealed,
        # then it is automatically verified.
        if column.reveal:
            return column.verify()

        if isinstance(node, Aggregate):
            return self._handle_aggregate(column, node)
        elif isinstance(node, Concat):
            return self._handle_concat(column, node)
        elif isinstance(node, Project):
            return self._handle_project(column, node)
        elif isinstance(node, Join):
            return self._handle_join(column, node)
        else:
            # other ops dont affect policy evaluation
            return self._continue_traversal(column, node)

    def _verify(self, policy):
        """
        Verification method. Will need to traverse the DAG generated by
        the protocol and compare it against the policy for the given PID.
        """

        root = self._find_root(policy)
        columns_to_verify = \
            [Column(policy["columns"][n.name]["read"], n.name, n.idx) for n in root.out_rel.columns]

        v = [self._verify_column(column, copy.deepcopy(root)) for column in columns_to_verify]
        vs = [c.verified for c in v]

        return all(vs)

    def verify(self):
        """
        Entry point for policy verification. Sets up networked peer if
        necessary and exchanges own policy with other parties before
        verifying the workflow against each policy individually.
        """

        if self.peer is None:
            self.setup_networked_peer()

        policies = self.peer.get_policies_from_others().values()

        return all([self._verify(policy) for policy in policies])


class Column:

    def __init__(self, reveal, name, idx):

        self.reveal = reveal
        self.name = name
        self.idx = idx
        self.verified = False
        self.current_rel_name = None

    def verify(self):

        self.verified = True
        return self

    def update_rel_name(self, node):

        self.current_rel_name = node.out_rel.name
        return self
