from __future__ import annotations

from dataclasses import dataclass

import grpc


@dataclass(slots=True)
class ServerTLSConfig:
    certificate_chain: bytes
    private_key: bytes
    root_certificates: bytes | None = None
    require_client_auth: bool = False

    def build_server_credentials(self):
        return grpc.ssl_server_credentials(
            private_key_certificate_chain_pairs=((self.private_key, self.certificate_chain),),
            root_certificates=self.root_certificates,
            require_client_auth=self.require_client_auth,
        )


@dataclass(slots=True)
class ClientTLSConfig:
    root_certificates: bytes | None = None
    private_key: bytes | None = None
    certificate_chain: bytes | None = None
    server_name_override: str | None = None

    def build_channel_credentials(self):
        return grpc.ssl_channel_credentials(
            root_certificates=self.root_certificates,
            private_key=self.private_key,
            certificate_chain=self.certificate_chain,
        )

    def build_channel_options(self) -> list[tuple[str, str]]:
        if not self.server_name_override:
            return []
        return [("grpc.ssl_target_name_override", self.server_name_override)]


__all__ = ["ClientTLSConfig", "ServerTLSConfig"]
