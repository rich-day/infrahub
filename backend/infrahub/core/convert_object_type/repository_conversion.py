from infrahub import lock
from infrahub.core.branch import Branch
from infrahub.core.convert_object_type.object_conversion import InputForDestField, convert_object_type
from infrahub.core.manager import NodeManager
from infrahub.core.node import Node
from infrahub.core.repositories.create_repository import RepositoryFinalizer
from infrahub.core.schema import NodeSchema
from infrahub.database import InfrahubDatabase


async def convert_repository_type(
    node: Node,
    target_schema: NodeSchema,
    mapping: dict[str, InputForDestField],
    branch: Branch,
    db: InfrahubDatabase,
    repository_post_creator: RepositoryFinalizer,
) -> Node:
    """Delete the node and return the new created one. If creation fails, the node is not deleted, and raise an error.
    An extra check is performed on input node peers relationships to make sure they are still valid."""

    repo_name = node.name.value  # type: ignore [attr-defined]
    async with lock.registry.get(name=repo_name, namespace="repository"):
        node_created = await convert_object_type(
            node=node,
            target_schema=target_schema,
            mapping=mapping,
            branch=branch,
            db=db,
        )

        # We can't apply post creation steps within `convert_object_type` transaction as a sdk call tries to fetch the node
        # created within the transaction from a different process, therefore that would not run within this transaction
        # so the node ends up being not found
        await repository_post_creator.post_create(
            branch=branch,
            obj=node_created,  # type: ignore
            db=db,
        )

        # Delete the RepositoryGroup associated with the old repository, as a new one was created for the new repository.
        repository_groups = (await node.groups_objects.get_peers(db=db)).values()  # type: ignore [attr-defined]
        for repository_group in repository_groups:
            await NodeManager.delete(db=db, branch=branch, nodes=[repository_group], cascade_delete=False)

    return node_created
