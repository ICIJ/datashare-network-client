from typing import List, Tuple

from dsnet.token import AbeToken, generate_commitments, generate_challenges, generate_pretokens, generate_tokens
from sscred import AbeParam, AbeSigner, AbePublicKey


def create_tokens(nb: int) -> Tuple[List[AbeToken], AbePublicKey]:
    sk, pk = AbeParam().generate_new_key_pair()
    signer = AbeSigner(sk, pk, disable_acl=True)
    coms, coms_internal = generate_commitments(signer, nb)
    challenges, challenges_int, token_skeys = generate_challenges(pk, coms)
    pre_tokens = generate_pretokens(signer, challenges, coms_internal)
    return generate_tokens(pk, challenges_int, token_skeys, pre_tokens), pk

