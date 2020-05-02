use crate::KStream;
use crate::stream::Change;

pub struct Filter<S, F>
    where S: KStream,
          F: Fn(&S::Key, &S::Value) -> bool
{
    pub(crate) stream: S,
    pub(crate) filt: F,
}

#[async_trait(?Send)]
impl<S, F> KStream for Filter<S, F>
    where S: KStream,
          F: Fn(&S::Key, &S::Value) -> bool
{
    type Key = S::Key;
    type Value = S::Value;

    async fn next(&mut self) -> Change<Self::Key, Self::Value> {
        loop {
            match self.stream.next().await {
                // Pass nulls as they are, they could be deletes
                Change::Item(p, k, Some(v)) => {
                    if (self.filt)(&k, &v) {
                        return Change::Item(p, k, Some(v));
                    }
                }

                x => return x,
            };
        }
    }
}