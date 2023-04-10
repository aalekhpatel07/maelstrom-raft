
mod envelope_builder {
    use crate::*;

    #[test]
    fn build_new_is_error_without_required_fields() {
        assert!(EnvelopeBuilder::<()>::new().build().is_err());
        assert!(EnvelopeBuilder::<()>::new().source("n1").build().is_err());
        assert!(EnvelopeBuilder::<()>::new().source("n1").destination("n2").build().is_err());
        assert!(EnvelopeBuilder::<()>::new().source("n1").message(()).build().is_err());
        assert!(EnvelopeBuilder::<()>::new().destination("n1").message(()).build().is_err());

        let err = EnvelopeBuilder::<()>::new().destination("n1").message(()).build().unwrap_err();
        assert_eq!(err, EnvelopeBuilderError::MissingField("source".into()));
    }

    #[test]
    fn build_new_ok_with_all_required_fields() {

        let envelope = EnvelopeBuilder::<()>::new().destination("d1").source("n1").message(()).build().unwrap();
        assert!(envelope.is_internal());
        assert_eq!(&envelope.source, "n1");
        assert_eq!(&envelope.destination, "d1");
        assert_eq!(envelope.message(), ());
    }

    #[test]
    fn reply() {
        let envelope = EnvelopeBuilder::<()>::new().destination("d1").source("n1").message(()).build().unwrap();
        assert!(envelope.is_internal());
        assert_eq!(&envelope.source, "n1");
        assert_eq!(&envelope.destination, "d1");
        assert_eq!(envelope.message(), ());

        let reply = envelope.reply(());
        assert!(!reply.is_internal());
        assert_eq!(&reply.source, "d1");
        assert_eq!(&reply.destination, "n1");
        assert_eq!(reply.message(), ());
    }

    #[test]
    fn send() {

        #[derive(serde::Serialize, serde::Deserialize, Debug, PartialEq, Eq, Clone)]
        struct Foo {
            bar: String
        }

        let msg = Foo { bar: "baz".to_string() };
        let envelope = EnvelopeBuilder::<Foo>::new().destination("d1").source("n1").message(msg.clone()).build().unwrap();
        assert!(envelope.is_internal());
        assert_eq!(&envelope.source, "n1");
        assert_eq!(&envelope.destination, "d1");
        assert!(envelope.body.in_reply_to().is_none());
        assert_eq!(envelope.message(), msg);
        let as_string = envelope.as_json_pretty().unwrap();
        assert!(as_string.contains("msg_id"));
        envelope.send().unwrap();

    }
}
