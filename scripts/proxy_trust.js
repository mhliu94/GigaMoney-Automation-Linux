// Add the exact capture server certificate to the Rust client's pin list.
// Keep the app's network flags, proxy choice, and existing certificates.
Java.perform(function () {
    const Client = Java.use('uniffi.engine_uniffi.HttpClient');
    const List = Java.use('java.util.ArrayList');
    const certificate = Java.use('android.util.Base64').decode(__CAPTURE_CA_DER__, 0);
    const configure = Client.setClient.overload('java.util.Map', 'java.lang.String', 'java.util.List');
    configure.implementation = function (flags, proxy, certificates) {
        const combined = List.$new();
        if (certificates !== null) combined.addAll(certificates);
        combined.add(certificate);
        const result = configure.call(this, flags, proxy, combined);
        send({event: 'rust_ca_applied', certificate_count: combined.size()});
        return result;
    };
    send({event: 'rust_ca_hook_installed'});
});
